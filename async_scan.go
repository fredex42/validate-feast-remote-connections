package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/davecgh/go-spew/spew"
)

func AsyncScanTable(ddbClient *dynamodb.Client, tableName *string, limit *int32) (chan CollectionRecord, chan RecipeRecord) {
	collectionsOut := make(chan CollectionRecord, 10)
	recipesOut := make(chan RecipeRecord, 10)

	var cursor map[string]types.AttributeValue

	go func() {
		for {
			result, err := ddbClient.Scan(context.Background(), &dynamodb.ScanInput{
				ExclusiveStartKey: cursor,
				TableName:         tableName,
				Limit:             limit,
			})
			if err != nil {
				log.Printf("ERROR scanning table: %s", err)
				break
			}
			for _, rec := range result.Items {
				maybeCollectionRecord := CollectionRecordFromDynamo(&rec)
				if maybeCollectionRecord != nil {
					collectionsOut <- *maybeCollectionRecord
				} else {
					maybeRecipeRecord := RecipeRecordFromDynamo(&rec)
					if maybeRecipeRecord != nil {
						recipesOut <- *maybeRecipeRecord
					} else {
						log.Printf("WARNING Unable to parse record: ")
						spew.Dump(rec)
					}
				}
			}
			if result.LastEvaluatedKey == nil {
				log.Print("INFO Table scan complete")
				break
			} else {
				cursor = result.LastEvaluatedKey
			}
		}

		close(collectionsOut)
		close(recipesOut)
	}()

	return collectionsOut, recipesOut
}
