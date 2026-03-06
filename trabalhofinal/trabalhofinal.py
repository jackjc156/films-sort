from raylibpy import *
import threading
import argparse
import csv
import time



############################################################################################### HashTable: separate chaining
class HashTable:
    def __init__(self, m=131071):
        self.m = int(m)
        self.table = [[] for _ in range(self.m)]
        self._size = 0

    def _hash(self, key):
        h = hash(key)
        return (h & 0x7FFFFFFFFFFFFFFF) % self.m

    def put(self, key, value):
        h = self._hash(key)
        bucket = self.table[h]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                bucket[i] = (k, value)
                return
        bucket.append((key, value))
        self._size += 1

    def get(self, key):
        h = self._hash(key)
        bucket = self.table[h]
        for k, v in bucket:
            if k == key:
                return v
        return None

    def contains(self, key):
        return self.get(key) is not None

    def append_to_listvalue(self, key, value):
        h = self._hash(key)
        bucket = self.table[h]
        for i, (k, v) in enumerate(bucket):
            if k == key:
                v.append(value)
                return
        bucket.append((key, [value]))
        self._size += 1

    def items(self):
        for bucket in self.table:
            for k, v in bucket:
                yield (k, v)

    def keys(self):
        for bucket in self.table:
            for k, _ in bucket:
                yield k

    def __len__(self):
        return self._size

############################################################################################################ MovieRecord
class MovieRecord:
    __slots__ = ('movieId', 'title', 'genres', 'rating_sum', 'rating_count', 'avg')

    def __init__(self, movieId, title, genres):
        self.movieId = int(movieId)
        self.title = title
        if isinstance(genres, str):
            self.genres = [g for g in genres.split('|') if g != '(no genres listed)'] if genres else []
        elif isinstance(genres, list):
            self.genres = genres
        else:
            self.genres = []
        self.genres = [g.strip().lower() for g in self.genres if g.strip() != '']
        self.rating_sum = 0.0
        self.rating_count = 0
        self.avg = 0.0

    def add_rating(self, r):
        self.rating_sum += float(r)
        self.rating_count += 1

    def finalize(self):
        if self.rating_count:
            self.avg = self.rating_sum / self.rating_count
        else:
            self.avg = 0.0

#################################################################################################################  Trie
class TrieNode:
    __slots__ = ('children', 'movie_ids', 'is_end')
    def __init__(self):
        self.children = {}
        self.movie_ids = []
        self.is_end = False

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, title, movieId, store_on_path=False):
        node = self.root
        t = title.lower()
        for ch in t:
            if ch not in node.children:
                node.children[ch] = TrieNode()
            node = node.children[ch]
            if store_on_path:
                node.movie_ids.append(movieId)
        node.is_end = True
        node.movie_ids.append(movieId)

    def starts_with(self, prefix):
        node = self.root
        p = prefix.lower()
        for ch in p:
            if ch not in node.children:
                return []
            node = node.children[ch]
        res = []
        stack = [node]
        while stack:
            n = stack.pop()
            if n.movie_ids:
                res.extend(n.movie_ids)
            for child in n.children.values():
                stack.append(child)
        return res

##################################################################### Indexes

class UserIndex:
    def __init__(self, table_size=262139):
        self.ht = HashTable(m=table_size)

    def add(self, userId, movieId, rating):
        key = int(userId)
        rec = self.ht.get(key)
        if rec is None:
            self.ht.put(key, [(int(movieId), float(rating))])
        else:
            rec.append((int(movieId), float(rating)))

    def get(self, userId):
        rec = self.ht.get(int(userId))
        if rec is None:
            return []
        return rec

class TagIndex:
    def __init__(self, table_size=131071):
        self.ht = HashTable(m=table_size)

    def add(self, tag, movieId):
        t = tag.strip().lower()
        if t == '':
            return
        self.ht.append_to_listvalue(t, int(movieId))

    def get(self, tag):
        return self.ht.get(tag.strip().lower()) or []


################################################################################################ Contador de linhas para barra de progresso

def count_lines(path):
    try:
        with open(path, 'rb') as f:
            # contar ocorrências de '\n'
            buf = f.read(1024 * 1024)
            count = 0
            while buf:
                count += buf.count(b'\n')
                buf = f.read(1024 * 1024)
        # subtrai 1 para header (se existir)
        return max(0, count - 1)
    except Exception:
        return 0

################################################################################################## processamento dos chunks
def process_ratings_chunk(rows):
    local_ratings = {}
    local_users = {}
    for uid, mid, r in rows:
        if mid not in local_ratings:
            local_ratings[mid] = [0.0, 0]
        local_ratings[mid][0] += r
        local_ratings[mid][1] += 1
        if uid not in local_users:
            local_users[uid] = []
        local_users[uid].append((mid, r))
    return local_ratings, local_users

########################################################################################################## build structures 
def build_structures(movies_csv, ratings_csv, tags_csv, chunksize=100_000, show_progress=True, state=None):
    t0 = time.perf_counter()

    movies_ht = HashTable(m=400009)
    trie = Trie()
    user_index = UserIndex()
    tag_index = TagIndex()

    # contar linhas p/ progresso real
    total_movies = count_lines(movies_csv) if movies_csv and state is not None else 0
    total_ratings = count_lines(ratings_csv) if ratings_csv and state is not None else 0
    total_tags = count_lines(tags_csv) if tags_csv and state is not None else 0

    w_movies = 0.10
    w_ratings = 0.80
    w_tags = 0.10

    
    ##################################################################################################### 1) movies.csv
    
    if state: state["stage"] = "Lendo movies.csv..."
    processed_movies = 0

    with open(movies_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                mid = int(row['movieId'])
                title = row.get('title', '')
                genres = row.get('genres', '')
            except:
                vals = list(row.values())
                if len(vals) < 3:
                    continue
                try:
                    mid = int(vals[0])
                except:
                    continue
                title = vals[1]
                genres = vals[2]

            mr = MovieRecord(mid, title, genres)
            movies_ht.put(mid, mr)
            trie.insert(title, mid, store_on_path=False)

            processed_movies += 1
            if state and total_movies > 0:
                state["progress"] = w_movies * (processed_movies / total_movies)

    
    #################################################################################################### 2) ratings.csv
    
    if state: state["stage"] = "Lendo ratings.csv..."
    processed_ratings = 0

    with open(ratings_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                uid = int(row['userId'])
                mid = int(row['movieId'])
                r = float(row['rating'])
            except:
                vals = list(row.values())
                if len(vals) < 3:
                    continue
                try:
                    uid = int(vals[0])
                    mid = int(vals[1])
                    r = float(vals[2])
                except:
                    continue

            mr = movies_ht.get(mid)
            if mr:
                mr.add_rating(r)

            user_index.add(uid, mid, r)

            processed_ratings += 1
            if state and total_ratings > 0 and processed_ratings % 1000 == 0:
                state["progress"] = w_movies + w_ratings * (processed_ratings / total_ratings)

    
    ########################################################################################################### 3) tags.csv
    
    if tags_csv:
        if state: state["stage"] = "Lendo tags.csv..."
        processed_tags = 0

        with open(tags_csv, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    mid = int(row['movieId'])
                    tag = row['tag']
                except:
                    vals = list(row.values())
                    if len(vals) < 3:
                        continue
                    try:
                        mid = int(vals[1])
                        tag = vals[2]
                    except:
                        continue

                tag_index.add(tag, mid)

                processed_tags += 1
                if state and total_tags > 0 and processed_tags % 1000 == 0:
                    state["progress"] = w_movies + w_ratings + w_tags * (processed_tags / total_tags)

    
    ############################################################################################################# 4) medias finais
    
    if state: state["stage"] = "Calculando médias..."

    total_movies_for_finalize = len(list(movies_ht.keys()))
    processed_finalize = 0

    for mid, mr in movies_ht.items():
        mr.finalize()
        processed_finalize += 1
        if state and total_movies_for_finalize > 0:
            state["progress"] = min(
                1.0,
                w_movies + w_ratings + w_tags + (processed_finalize / total_movies_for_finalize) * 0.05
            )

    if state: state["stage"] = "Concluído"
    t1 = time.perf_counter()
    return movies_ht, trie, user_index, tag_index



################################################################################################################ consultas e formatacao (agora retornam listas de strings)

def format_movie_row_cols(mr):
    genres = ", ".join(mr.genres) if mr.genres else "-"
    return [
        str(mr.movieId),
        mr.title[:50],
        genres[:20],
        f"{mr.avg:.4f}",
        str(mr.rating_count)
    ]


################################################################################################################ pesquisa prefixos

def query_prefix(trie, movies_ht, prefix, limit=200):
    lines = []
    ids = trie.starts_with(prefix)
    unique = {}
    for mid in ids:
        unique[mid] = True
    mids = list(unique.keys())
    recs = []
    for mid in mids:
        mr = movies_ht.get(mid)
        if mr:
            recs.append(mr)
    recs.sort(key=lambda x: (-x.avg, -x.rating_count, x.title))
    total = len(recs)
    if total == 0:
        lines.append("Nenhum filme encontrado para o prefixo informado.")
        return lines
    to_show = recs[:limit]
    lines.append(f"Mostrando {len(to_show)} de {total} resultados para prefixo '{prefix}':")
    lines.append(["movieId", "title", "genres", "avg", "count"])

    

    for mr in to_show:
        lines.append(format_movie_row_cols(mr))
    if total > limit:
        lines.append(f"... (omitidos {total - limit} resultados)")
    return lines


################################################################################################################ pesquisa usuarios
def query_user(user_index, movies_ht, userId, limit=20):
    lines = []
    recs = user_index.get(userId)
    if not recs:
        lines.append(f"Nenhum rating encontrado para o usuário {userId}.")
        return lines
    full = []
    for (mid, ur) in recs:
        mr = movies_ht.get(mid)
        if mr:
            full.append((mid, ur, mr.avg, mr.rating_count, mr.title))
        else:
            full.append((mid, ur, 0.0, 0, "<movie not in movies.csv>"))
    full.sort(key=lambda x: (-x[1], -x[2], -x[3]))
    lines.append(f"Ratings do usuário {userId} (mostrando até {limit}):")
    lines.append(["movieId", "title", "user_rate", "glob_avg", "count"])

    

    for item in full[:limit]:
        mid, ur, gavg, cnt, title = item
        lines.append([
            str(mid),
            title[:50],
            f"{ur:.4f}",
            f"{gavg:.4f}",
            str(cnt)
        ])


    return lines

####################################################################################################################### pesquisa top generos
def query_top_genre(movies_ht, N, genre, mincount=1000):
    lines = []
    g = genre.strip().lower()
    results = []
    for mid, mr in movies_ht.items():
        if mr is None:
            continue
        if g in mr.genres and mr.rating_count >= mincount:
            results.append(mr)
    results.sort(key=lambda x: (-x.avg, -x.rating_count, x.title))
    lines.append(f"Top {N} filmes do gênero '{genre}' (mincount={mincount}):")
    lines.append(["movieId", "title", "genres", "avg", "count"])


    for mr in results[:N]:
        lines.append(format_movie_row_cols(mr))
    if len(results) == 0:
        lines.append("Nenhum filme encontrado para esse gênero (verifique ortografia).")
    return lines


#################################################################################################################### pesquisa tags
def query_tags(tag_index, movies_ht, tags_list, limit=200):
    lines = []
    tags_list = [t.strip().lower() for t in tags_list if t.strip() != ""]
    if not tags_list:
        lines.append("Forneça pelo menos uma tag.")
        return lines
    lists = []
    for t in tags_list:
        lst = tag_index.get(t)
        if not lst:
            lines.append(f"Nenhum filme encontrado para a tag '{t}'. Interseção vazia.")
            return lines
        lists.append(lst)
    lists.sort(key=lambda x: len(x))
    s = set(lists[0])
    for lst in lists[1:]:
        s.intersection_update(lst)
        if not s:
            lines.append("Interseção vazia — nenhum filme tem todas as tags informadas.")
            return lines
    recs = []
    for mid in s:
        mr = movies_ht.get(mid)
        if mr:
            recs.append(mr)
    recs.sort(key=lambda x: (-x.avg, -x.rating_count, x.title))
    total = len(recs)
    lines.append(f"Filmes com tags {tags_list}: {total} resultados. Mostrando até {limit}:")
    lines.append(["movieId", "title", "genres", "avg", "count"])


    for mr in recs[:limit]:
        lines.append(format_movie_row_cols(mr))
    if total > limit:
        lines.append(f"... (omitidos {total - limit} resultados)")
    return lines

######################################################################################### status
def print_stats_lines(movies_ht, user_index, tag_index):
    lines = []
    total_movies = len(list(movies_ht.keys()))
    total_users = len(user_index.ht)
    total_tags = len(tag_index.ht)
    avg_ratings_per_movie = 0.0
    cnt = 0
    for mid, mr in movies_ht.items():
        avg_ratings_per_movie += mr.rating_count
        cnt += 1
    if cnt:
        avg_ratings_per_movie /= cnt
    lines.append("ESTATISTICAS:")
    lines.append(f"  Filmes carregados: {total_movies}")
    lines.append(f"  Usuários indexados (estima): {total_users}")
    lines.append(f"  Tags distintas (estima): {total_tags}")
    lines.append(f"  Média de ratings por filme: {avg_ratings_per_movie:.2f}")
    return lines


########################################################################################### helper para saída no UI

def append_output(state, lines):
    if "output_lines" not in state:
        state["output_lines"] = []
        state["output_offset"] = 0
    for l in lines:
        # dividir linhas muito longas por largura razoável (simplesmente)
        state["output_lines"].append(l)
    # limitar tamanho para não crescer indefinidamente
    MAX_LINES = 2000
    if len(state["output_lines"]) > MAX_LINES:
        state["output_lines"] = state["output_lines"][-MAX_LINES:]
        state["output_offset"] = max(0, state.get("output_offset",0) - (len(state["output_lines"]) - MAX_LINES))


########################################################################### organizador do build

def build_async(args, state):
    start = time.perf_counter()
    try:
        movies_ht, trie, user_index, tag_index = build_structures(
            args.movies,
            args.ratings,
            args.tags,
            chunksize=args.chunksize,
            show_progress=not args.no_progress,
            state=state
        )
        state["movies_ht"] = movies_ht
        state["trie"] = trie
        state["user_index"] = user_index
        state["tag_index"] = tag_index
        state["elapsed"] = time.perf_counter() - start
        state["done"] = True
        state["progress"] = 1.0
    except Exception as e:
        state["error"] = str(e)
        state["done"] = True

####################################################################################################### buffer para output
def ui_log(state, text):
    # cria o buffer automaticamente se não existir
    if "output_lines" not in state:
        from collections import deque
        state["output_lines"] = deque(maxlen=400)

    for line in str(text).split("\n"):
        state["output_lines"].append(line)


################################################################################################# main + UI raylib
def main():
    p = argparse.ArgumentParser(description="Programa de busca/consulta sobre filmes (com UI de progresso).")
    p.add_argument("--movies", default="movies.csv")
    p.add_argument("--ratings", default="ratings.csv")
    p.add_argument("--tags", default="tags.csv")
    p.add_argument("--chunksize", type=int, default=100_000)
    p.add_argument("--no-progress", action='store_true')
    args = p.parse_args()

    init_window(1060, 820, "Movies - Loader + Console")
    set_target_fps(60)

    started = False
    from collections import deque

    state = {
        "done": False,
        "error": None,
        "elapsed": 0.0,
        "progress": 0.0,
        "stage": "Aguardando início...",
        "output_lines": deque(maxlen=400),

        "movies_ht": None,
        "trie": None,
        "user_index": None,
        "tag_index": None
    }
    worker_thread = None

    cmd_buffer = ""
    max_len = 400
    last_command = ""

    # UI layout params
    bar_x = 20; bar_y = 160; bar_w = 960; bar_h = 28
    output_x = 20; output_y = 220; output_w = 1000; output_h = 460
    line_h = 18
    visible_lines = output_h // line_h

    while not window_should_close():
        begin_drawing()
        clear_background(BLACK)

        draw_text("Organizador de filmes", 350, 20, 35, BLUE)
        draw_text("2000", 725, 45, 10, BLUE)
        # instruções pequenas
        draw_text(f"movies: {args.movies}", 20, 50, 12, GREEN)
        draw_text(f"ratings: {args.ratings}", 20, 66, 12, GREEN)
        draw_text(f"tags: {args.tags}", 20, 82, 12, GREEN)
        draw_text(f"chunksize: {args.chunksize}", 20, 98, 12, GREEN)

        if not started:
            draw_text("Pressione F para iniciar a construção (ou ESC para sair)", 20, 122, 14, BLUE)
        if is_key_pressed(KEY_F) and not started:
            started = True
            worker_thread = threading.Thread(target=build_async, args=(args, state), daemon=True)
            worker_thread.start()

        # barra de progresso
        progress = state.get("progress", 0.0)
        draw_rectangle(bar_x, bar_y, bar_w, bar_h, LIGHTGRAY)
        draw_rectangle(bar_x, bar_y, int(bar_w * progress), bar_h, SKYBLUE)
        draw_text(f"{int(progress*100)}%", bar_x + bar_w//2 - 20, bar_y + 4, 16, BLACK)
        stage_txt = state.get("stage", "")
        draw_text(f"Etapa: {stage_txt}", 200, 200, 10, GREEN)
        # status text
        if not started:
            draw_text("Status: preparado", 20, 200, 12, GREEN)
        else:
            if not state["done"]:
                draw_text("Status: carregando...", 20, 200, 12, GREEN)
            else:
                if state.get("error"):
                    draw_text(f"Erro: {state['error']}", 20, 200, 12, RED)
                else:
                    draw_text(f"Concluído em {state['elapsed']:.2f}s", 20, 200, 12, DARKGREEN)

        # output area background
        draw_rectangle_lines(output_x-2, output_y-2, output_w+4, output_h+4, GRAY)
        draw_rectangle(output_x, output_y, output_w, output_h, BLACK)

        # handle scrolling input for output
        # arrow up / down and page up/down
        if is_key_pressed(KEY_UP):
            state["output_offset"] = max(0, state.get("output_offset", 0) - 1)
        if is_key_pressed(KEY_DOWN):
            max_off = max(0, len(state["output_lines"]) - visible_lines)
            state["output_offset"] = min(max_off, state.get("output_offset", 0) + 1)
        if is_key_pressed(KEY_PAGE_UP):
            state["output_offset"] = max(0, state.get("output_offset", 0) - visible_lines)
        if is_key_pressed(KEY_PAGE_DOWN):
            max_off = max(0, len(state["output_lines"]) - visible_lines)
            state["output_offset"] = min(max_off, state.get("output_offset", 0) + visible_lines)

        # render output lines
                # render output lines
        off = state.get("output_offset", 0)
        for i in range(visible_lines):
            idx = off + i
            if idx >= len(state["output_lines"]):
                break

            raw_line = state["output_lines"][idx]

            # Decidir cor baseado em conteúdo textual (se for lista, usa o primeiro campo)
            color = GREEN
            sample = ""
            if isinstance(raw_line, str):
                sample = raw_line.lower()
            else:
                # se for lista/tupla, convertemos apenas o primeiro campo para decidir cor
                try:
                    sample = str(raw_line[0]).lower()
                except Exception:
                    sample = ""

            if sample.startswith("mostrando"):
                color = BLUE
            elif sample.startswith("ratings do usuário"):
                color = DARKGREEN
            elif sample.startswith("filmes com tags"):
                color = PURPLE
            elif sample.startswith("top "):
                color = MAROON
            elif sample.startswith("estatisticas") or sample.startswith("estatísticas"):
                color = SKYBLUE
            else:
                color = GREEN

            # Render: se for string, desenha inteira; se for lista (colunas), desenha por colunas
            y = output_y + 2 + i * line_h
            if isinstance(raw_line, str):
                draw_text(raw_line, output_x + 4, y, 14, color)
            else:
                # lista de colunas: posições fixas (ajuste se necessário)
                x_positions = [output_x + 4, output_x + 120, output_x + 620, output_x + 840, output_x + 920]
                for j, col in enumerate(raw_line):
                    if j >= len(x_positions):
                        # se houver mais colunas do que posições, desenha concatenado ao fim
                        draw_text(str(col), x_positions[-1] + 8 + (j - len(x_positions)) * 80, y, 14, color)
                    else:
                        draw_text(str(col), x_positions[j], y, 14, color)


        # input prompt (only active after build success)
        if state["done"] and not state.get("error"):
            draw_text("Comando (ex: prefixo star, user 1, top 10 action, tags drama funny):", 20, output_y + output_h + 8, 14, GREEN)
            draw_rectangle_lines(20, output_y + output_h + 30, 960, 28, GRAY)
            draw_text(cmd_buffer, 24, output_y + output_h + 34, 14, DARKGREEN)
           
            # read typed chars
            key = get_char_pressed()
            while key > 0:
                if 32 <= key <= 126 and len(cmd_buffer) < max_len:
                    cmd_buffer += chr(key)
                key = get_char_pressed()
            if is_key_pressed(KEY_BACKSPACE):
                cmd_buffer = cmd_buffer[:-1]

            if is_key_pressed(KEY_ENTER):
                last_command = cmd_buffer.strip()
                cmd = last_command.lower()

                if cmd == "exit" or cmd == "quit":
                    close_window()

                elif cmd.startswith("prefixo "):
                    prefix = last_command[8:]
                    lines = query_prefix(state["trie"], state["movies_ht"], prefix)
                    append_output(state, lines)

                elif cmd.startswith("user "):
                    try:
                        uid = int(last_command[5:])
                        lines = query_user(state["user_index"], state["movies_ht"], uid)
                        append_output(state, lines)
                    except:
                        append_output(state, ["user <id> inválido"])

                elif cmd.startswith("top "):
                    parts = last_command.split()
                    if len(parts) >= 3:
                        try:
                            n = int(parts[1]); genero = parts[2]
                            lines = query_top_genre(state["movies_ht"], n, genero)
                            append_output(state, lines)
                        except:
                            append_output(state, ["uso: top N genero"])
                    else:
                        append_output(state, ["uso: top N genero"])

                elif cmd.startswith("tags "):
                    tags = last_command[5:].split()
                    lines = query_tags(state["tag_index"], state["movies_ht"], tags)
                    append_output(state, lines)

                elif cmd == "stats":
                    lines = print_stats_lines(state["movies_ht"], state["user_index"], state["tag_index"])
                    append_output(state, lines)

                else:
                    append_output(state, [f"Comando não reconhecido: '{last_command}'. Digite prefixo/user/top/tags/stats/exit"])

                cmd_buffer = ""

        # small help
        draw_text("Navegação do output: ↑ / ↓ / PageUp / PageDown", 20, output_y + output_h + 70, 12, DARKGRAY)

        end_drawing()

    close_window()

if __name__ == "__main__":
    main()


