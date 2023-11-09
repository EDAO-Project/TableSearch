# User Study
The user study is performed using a Django website.
Users can create a username and annotate a set of tables for a given set of queries.

## Setup
Build the Docker image

```bash
docker build -t user-study .
```

You now need to specify the queries and tables to annotate.
This information should be in `params.json`.
Open the file, and generate your own with your data with the same file name.
You can remove template `params.json`.

A query file should be on the same format as in the example below:

```json
{
    "queries": [
        ["http://dbpedia.org/resource/1996_in_sports", "http://dbpedia.org/resource/Jerry_D._Bailey", "http://dbpedia.org/resource/United_States"],
        [
            "http://dbpedia.org/resource/1985_Formula_One_season", "http://dbpedia.org/resource/Niki_Lauda", "http://dbpedia.org/resource/McLaren"]
    ]
}
```

A table file should be on the same format as in the example below:

```json
{ 
    "pgTitle": "Nebraska Highway 8",
    "headers": [
        {"text": "County", "isNumeric": false, "links": []}, {"text": "Location", "isNumeric": false, "links": []}, {"text": "Mile", "isNumeric": false, "links": []}, {"text": "Junction", "isNumeric": false, "links": []}, {"text": "Notes", "isNumeric": false, "links": []}
        ],
    "rows": [
        [{"text": "Nuckolls", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Nuckolls_County,_Nebraska"]}, {"text": "Superior", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Superior,_Nebraska"]}, {"text": "0.00", "isNumeric": false, "links": []}, {"text": "Category:Jct template transclusions with missing shields", "isNumeric": false, "links": []}, {"text": "Western terminus", "isNumeric": false, "links": []}], 
        [{"text": "Thayer", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Thayer_County,_Nebraska"]}, {"text": "Byron", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Byron,_Nebraska"]}, {"text": "16.01", "isNumeric": false, "links": []}, {"text": "Category:Jct template transclusions with missing shields", "isNumeric": false, "links": []}, {"text": "", "isNumeric": false, "links": []}],
        [{"text": "Thayer", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Thayer_County,_Nebraska"]}, {"text": "Chester", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Chester,_Nebraska"]}, {"text": "24.10", "isNumeric": false, "links": []}, {"text": "Category:Jct template transclusions with missing shields", "isNumeric": false, "links": []}, {"text": "", "isNumeric": false, "links": []}],
        [{"text": "Richardson", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Richardson_County,_Nebraska"]}, {"text": "Falls City", "isNumeric": false, "links": ["http://www.wikipedia.org/wiki/Falls_City,_Nebraska"]}, {"text": "148.88", "isNumeric": false, "links": []}, {"text": "Category:Jct template transclusions with missing shields", "isNumeric": false, "links": []}, {"text": "Eastern terminus", "isNumeric": false, "links": []}]
    ]
}
```

## Running the User Study Website
Create a container for the user study website

```bash
mkdir -p data/
docker build -t user-study .
docker run -v ${PWD}/data:/home/data -p 8000:8000 user-study
```

Users can now open `http://localhost:8000/` to start annotating tables.
To annotate remotely, retrieve the IP address of the server by running `hostname -I` on the server, and substitute `localhost` with the IP address in your browser.