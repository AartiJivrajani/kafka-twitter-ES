package common

const (
	KeyWord   = "bitcoin"
	ESIndex   = "twitter"
	ESType    = "tweet"
	ESMapping = `{
  "mappings": {
    "tweet": {
      "properties": {
        "msg": {
          "type": "keyword"
        }
      }
    }
  }
}`
)
