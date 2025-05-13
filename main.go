package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func limitVal(from *int) *int32 {
	if *from == -1 {
		return nil
	}
	var realLimit = int32(*from)
	return &realLimit
}

func receiver(collectionsChan chan CollectionRecord, recipesChan chan RecipeRecord) (*FeastUsers, *FeastCollections) {
	tree := make(FeastUsers, 0)
	recipes := make(FeastCollections, 0)

	openChans := 2
	for {
		select {
		case rec, haveMore := <-collectionsChan:
			if !haveMore {
				log.Printf("collections channel closed")
				openChans -= 1
				if openChans == 0 {
					return &tree, &recipes
				}
			}
			collection := FeastCollectionFromRecord(&rec)
			if collection != nil {
				if existingList, haveExisting := tree[collection.Owner]; haveExisting {
					updated := append(existingList, *collection)
					tree[collection.Owner] = updated
				} else {
					tree[collection.Owner] = make([]FeastCollection, 1)
					tree[collection.Owner][0] = *collection
				}
			}
		case rec, haveMore := <-recipesChan: //ignore for the moment
			if !haveMore {
				log.Printf("recipes channel closed")
				openChans -= 1
				if openChans == 0 {
					return &tree, &recipes
				}
			}

			recep := FeastSavedRecipeFromRecord(&rec)
			if existingCollection, haveExisting := recipes[recep.CollectionId]; haveExisting {
				updated := append(existingCollection, *recep)
				recipes[recep.CollectionId] = updated
			} else {
				recipes[recep.CollectionId] = make([]FeastSavedRecipe, 1)
				recipes[recep.CollectionId][0] = *recep
			}
		}
	}

}

func main() {
	tableName := flag.String("table", "feast-collections-content-table-CODE", "table to use")
	limit := flag.Int("limit", -1, "only process this many records. Defaults to -1 => process all")
	flag.Parse()

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load configuration, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	log.Printf("Scanning %s...", *tableName)
	collectionsChan, recipesChan := AsyncScanTable(client, tableName, limitVal(limit))

	users, collections := receiver(collectionsChan, recipesChan)
	//spew.Dump(results)

	log.Printf("Reconciling...")
	//How many users do we have in total?
	userCount := len(*users)
	affectedCount := 0
	//OK now let's find all users who have more than 2 collections....
	for uid, userCollections := range *users {
		if len(userCollections) > 2 {
			affectedCount += 1
			log.Printf("User with ID %d has %d collections:", uid, len(userCollections))
			for _, c := range userCollections {
				collectionContent := (*collections)[c.CollectionID]
				log.Printf("\t%s %s %s (%d items)", c.CollectionID, c.CollectionType, c.LastModified.Format(time.RFC3339Nano), len(collectionContent))
			}
		}
	}
	log.Printf("A total of %d users out of %d were affected, that's %.1f%%", affectedCount, userCount, (float64(affectedCount)/float64(userCount))*100)
}
