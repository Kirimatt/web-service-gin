package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

var (
// cassandraHost     string = os.Getenv("CASSANDRA_HOST")
// cassandraUser     string = os.Getenv("CASSANDRA_USER")
// cassandraPassword string = os.Getenv("CASSANDRA_PASSWORD")
)

// album represents data about a record album.
type album struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Artist string  `json:"artist"`
	Price  float64 `json:"price"`
}

type tweet struct {
	ID       string `json:"id"`
	Timeline string `json:"timeline"`
	Text     string `json:"text"`
}

// albums slice to seed record album data.
var albums = []album{
	{ID: "1", Title: "Blue Train", Artist: "John Coltrane", Price: 56.99},
	{ID: "2", Title: "Jeru", Artist: "Gerry Mulligan", Price: 17.99},
	{ID: "3", Title: "Sarah Vaughan and Clifford Brown", Artist: "Sarah Vaughan", Price: 39.99},
}

func main() {
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	router := gin.Default()
	router.GET("/albums", getAlbums)
	router.GET("/albums/:id", getAlbumByID)
	router.POST("/albums", postAlbums)
	router.GET("/tweets", getTweets(session))
	router.GET("/tweets/:id", getTweetsByID(session))
	router.POST("/tweets", postTweets(session))

	router.Run("localhost:8080")
}

func postTweets(session *gocql.Session) gin.HandlerFunc {
	fn := func(c *gin.Context) {
		var newTweet tweet

		if err := c.BindJSON(&newTweet); err != nil {
			log.Fatal(err)
			return
		}

		newTweet.ID = gocql.TimeUUID().String()
		fmt.Println("Tweet:", newTweet.ID, newTweet.Text)

		if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
			newTweet.Timeline, newTweet.ID, newTweet.Text).Exec(); err != nil {
			log.Fatal(err)
		}

		fmt.Println("inserted")

		c.IndentedJSON(http.StatusCreated, newTweet)
	}

	return gin.HandlerFunc(fn)
}

func getTweets(session *gocql.Session) gin.HandlerFunc {
	fn := func(c *gin.Context) {
		scanner := session.Query(`SELECT id, text FROM tweet`).Iter().Scanner()
		var result []tweet
		for scanner.Next() {
			var searchTweet tweet
			var err = scanner.Scan(&searchTweet.ID, &searchTweet.Text)
			if err != nil {
				log.Fatal(err)
			}
			result = append(result, searchTweet)
			fmt.Println("Tweet:", searchTweet)
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		c.IndentedJSON(http.StatusOK, result)
	}

	return gin.HandlerFunc(fn)
}

func getTweetsByID(session *gocql.Session) gin.HandlerFunc {
	fn := func(c *gin.Context) {
		timeline := c.Param("timeline")

		var searchTweet tweet = tweet{Timeline: timeline}
		if err := session.Query(`SELECT id, text, timeline FROM tweet WHERE timeline = ? LIMIT 1`,
			timeline).Consistency(gocql.One).Scan(&searchTweet.ID, &searchTweet.Text, &searchTweet.Timeline); err != nil {
			log.Fatal(err)
			c.IndentedJSON(http.StatusNotFound, gin.H{"message": "unexpected error"})
		}

		fmt.Println("Tweet:", searchTweet)
		c.IndentedJSON(http.StatusCreated, searchTweet)
	}

	return gin.HandlerFunc(fn)
}

// getAlbums responds with the list of all albums as JSON.
func getAlbums(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, albums)
}

// postAlbums adds an album from JSON received in the request body.
func postAlbums(c *gin.Context) {
	var newAlbum album

	// Call BindJSON to bind the received JSON to
	// newAlbum.
	if err := c.BindJSON(&newAlbum); err != nil {
		return
	}

	// Add the new album to the slice.
	albums = append(albums, newAlbum)
	c.IndentedJSON(http.StatusCreated, newAlbum)
}

// getAlbumByID locates the album whose ID value matches the id
// parameter sent by the client, then returns that album as a response.
func getAlbumByID(c *gin.Context) {
	id := c.Param("id")

	// Loop through the list of albums, looking for
	// an album whose ID value matches the parameter.
	for _, a := range albums {
		if a.ID == id {
			c.IndentedJSON(http.StatusOK, a)
			return
		}
	}
	c.IndentedJSON(http.StatusNotFound, gin.H{"message": "album not found"})
}
