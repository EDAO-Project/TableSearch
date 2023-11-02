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

## Running the User Study Website
Create a container for the user study website

```bash
mkdir -p data/
docker run -v ${PWD}/data:/data -p 8000:8000 user-study
```

Users can now open `http://localhost:8000/` to start annotating tables.
To annotate remotely, retrieve the IP address of the server by running `hostname -I` on the server, and substitute `localhost` with the IP address in your browser.