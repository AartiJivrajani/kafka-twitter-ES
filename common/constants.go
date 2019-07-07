package common

const (
	KeyWord   = "bitcoin"
	ESIndex   = "twitter"
	ESType    = "tweet"
	ESMapping = `{
  "mappings": {
    "tweet": {
      "properties": {
        "_doc": {
          "type": "keyword"
        }
      }
    }
  }
}`
)
