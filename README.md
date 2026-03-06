# Movie Data Search Engine

This project implements a **movie search and indexing system** using custom data structures and an interactive graphical interface.  
It processes large movie datasets (CSV files) and allows fast queries by title prefix, user ratings, genres, and tags.

The system was developed as part of a **Data Structures and Algorithms project**, focusing on efficient indexing and retrieval of large datasets.

---

# Features

- Custom **Hash Table (Separate Chaining)** implementation
- **Trie (Prefix Tree)** for efficient prefix search on movie titles
- **User rating index** for retrieving movies rated by a specific user
- **Tag-based search system**
- **Average rating computation** per movie
- **Graphical interface** for interaction
- **Progress bar** during dataset loading
- Efficient processing of **large CSV files**

---

# Data Structures Used

## Hash Table

A custom **hash table with separate chaining** is used to store and retrieve movie-related information efficiently.

Used for:
- Movie metadata
- User rating index
- Tag index

Structure example:

```
hash(key) → bucket → linked list of entries
```

Average lookup time is constant for well-distributed hashes.

---

## Trie (Prefix Tree)

A **Trie** is used for efficient **prefix search on movie titles**.

Example:

Searching for:

```
star
```

May return:

- Star Wars
- Star Trek
- Stardust

This structure allows searching titles **character by character**, avoiding full dataset scans.

---

## User Rating Index

Stores ratings grouped by user:

```
userId → [(movieId, rating), ...]
```

This allows efficient retrieval of all movies rated by a specific user.

---

## Tag Index

Stores movie IDs grouped by tag:

```
tag → [movieId, movieId, ...]
```

This enables multi-tag searches.

Example:

```
tags comedy funny
```

Returns movies that contain **both tags**.

---

# Dataset Files

The program uses three CSV files.

## movies.csv

Contains movie metadata.

Example:

```
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
```

---

## ratings.csv

Contains user ratings.

Example:

```
userId,movieId,rating,timestamp
1,296,5.0,1147880044
```

---

## tags.csv

Contains user tags.

Example:

```
userId,movieId,tag,timestamp
2,60756,funny,1445714994
```

---

# Installation

Install required dependencies:

```bash
pip install raylibpy
```

---

# Running the Program

Run the program using:

```bash
python main.py
```

Optional arguments:

```
--movies <path>
--ratings <path>
--tags <path>
--chunksize <number>(disabled by libraries limitation)
--no-progress
```

Example:

```bash
python main.py --movies movies.csv --ratings ratings.csv --tags tags.csv
```

---

# Interface

When the program starts:

1. Press **F** to begin loading datasets.
2. A **progress bar** will appear during loading.
3. After loading finishes, the **command console** becomes available.

Navigation keys:

```
↑ / ↓       scroll output
PageUp/PageDown
```

---

# Available Commands

## Prefix Search

Search movies by title prefix.

```
prefixo star
```

Returns movies whose titles start with **"star"**.

---

## User Ratings

Retrieve movies rated by a specific user.

```
user 1
```

Shows the movies rated by that user.

---

## Top Movies by Genre

```
top 10 action
```

Displays the **top 10 movies** in the action genre.

---

## Tag Search

Search movies with specific tags.

```
tags drama funny
```

Returns movies containing **both tags**.

---

## Statistics

```
stats
```

Displays general dataset statistics.

---

## Exit

```
exit
```

Closes the program.

---

# Algorithm Complexity

Below are the expected **time complexities** for the main operations.

| Operation | Data Structure | Time Complexity |
|-----------|---------------|----------------|
Insert movie | Hash Table | **O(1)** average |
Lookup movie by ID | Hash Table | **O(1)** average |
Prefix search | Trie | **O(k)** |
Insert title into Trie | Trie | **O(k)** |
Retrieve user ratings | Hash Table | **O(1)** average |
Retrieve movies by tag | Hash Table | **O(1)** average |
Multi-tag intersection | Lists/Sets | **O(n)** |
Compute average rating | Aggregation | **O(r)** |

Where:

- **k** = length of the search prefix
- **n** = number of movies associated with a tag
- **r** = number of ratings for a movie

### Loading Complexity

Dataset loading requires processing all records:

| Dataset | Complexity |
|-------|-------------|
Movies | **O(M)** |
Ratings | **O(R)** |
Tags | **O(T)** |

Where:

- **M** = number of movies
- **R** = number of ratings
- **T** = number of tags

Total loading complexity:

```
O(M + R + T)
```

---

# Performance Considerations

The system is optimized for **large datasets** using:

- Hash-based indexing
- Prefix tree search
- Chunked CSV processing
- Precomputed indexes

These optimizations significantly reduce query time compared to linear scans.

---

# Author

Me, for an academic project developed for **Data Structures and Algorithms coursework**.
