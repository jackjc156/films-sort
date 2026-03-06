# Movie Data Search Engine

This project implements a movie search system using custom data structures.  
It processes movie datasets (CSV files) and allows fast queries by title prefix, user ratings, genres, and tags.

The main goal of the project is to explore indexing techniques and data structures for efficient search in large datasets.

---

# Features

- Hash table implementation using separate chaining
- Trie (prefix tree) for title prefix search
- Index of movies rated by each user
- Tag-based search
- Average rating computation
- Graphical interface using raylib
- Progress bar during dataset loading

---

# Data Structures

## Hash Table

A custom hash table with separate chaining is used to store and retrieve movie-related data efficiently.

It is used for:

- movie metadata
- user rating index
- tag index

Structure:


hash(key) → bucket → linked list


Average lookup time is **O(1)** assuming a well-distributed hash.

---

## Trie (Prefix Tree)

A Trie is used to support efficient prefix search on movie titles.

Example:

Searching for


star


may return:

- Star Wars
- Star Trek
- Stardust

The Trie allows searching titles character by character without scanning the entire dataset.

---

## User Rating Index

Ratings are grouped by user:


userId → [(movieId, rating), ...]


This allows retrieving all movies rated by a specific user efficiently.

---

## Tag Index

Tags are stored as a mapping from tag to movie IDs:


tag → [movieId, movieId, ...]


Example query:


tags comedy funny


Returns movies containing **both tags**.

---

# Dataset Files

The program expects three CSV files.

### movies.csv

Contains movie metadata.

Example:


movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy


---

### ratings.csv

Contains user ratings.

Example:


userId,movieId,rating,timestamp
1,296,5.0,1147880044


---

### tags.csv

Contains user-generated tags.

Example:


userId,movieId,tag,timestamp
2,60756,funny,1445714994


---

# Installation

Install the required dependency:

bash
pip install raylibpy
Running the Program

Run the program with:

python main.py

Optional dataset paths can be provided:

python main.py --movies movies.csv --ratings ratings.csv --tags tags.csv
Interface

When the program starts:

Press F to start loading the datasets.

A progress bar will be displayed.

After loading finishes, the command console becomes available.

Navigation keys:

↑ / ↓          scroll output
PageUp/PageDown
Available Commands
Prefix Search
prefixo star

Searches for movies whose titles start with "star".

User Ratings
user 1

Displays movies rated by the specified user.

Top Movies by Genre
top 10 action

Displays the top 10 movies in the given genre.

Tag Search
tags drama funny

Returns movies containing both tags.

Statistics
stats

Displays general statistics about the dataset.

Exit

Closes the program.

Algorithm Complexity
Operation	Data Structure	Complexity
Insert movie	Hash table	O(1) average
Lookup movie by ID	Hash table	O(1) average
Insert title	Trie	O(k)
Prefix search	Trie	O(k)
Retrieve user ratings	Hash table	O(1) average
Retrieve movies by tag	Hash table	O(1) average
Tag intersection	lists/sets	O(n)
Compute average rating	aggregation	O(r)

Where:

k = length of the prefix

n = number of movies associated with a tag

r = number of ratings for a movie

Loading Complexity

The loading phase processes all dataset entries:

O(M + R + T)

Where:

M = number of movies

R = number of ratings

T = number of tags

Author

Me, project developed for a Data Structures and sorting course.
