package main

import (
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
)

type FeastCollection struct {
	CollectionID   uuid.UUID
	Owner          int64
	CollectionType string
	ContentType    string
	LastModified   time.Time
}

func FeastCollectionFromRecord(rec *CollectionRecord) *FeastCollection {
	userId, err := strconv.ParseInt(rec.UserAndCollection, 10, 32)
	if err != nil {
		log.Printf("ERROR Invalid user id: '%s'", rec.UserAndCollection)
		spew.Dump(rec)
		return nil
	}
	collectionId, err := uuid.Parse(rec.ReferencedObjectId)
	if err != nil {
		log.Printf("ERROR: Invalid collection ID: %s", rec.ReferencedObjectId)
		return nil
	}

	t, err := time.Parse(time.RFC3339, rec.LastModified)
	if err != nil {
		log.Printf("ERROR: Invalid timestamp %s", rec.LastModified)
		return nil
	}

	return &FeastCollection{
		CollectionID:   collectionId,
		Owner:          userId,
		CollectionType: rec.CollectionType,
		ContentType:    rec.ContentType,
		LastModified:   t,
	}
}

type FeastSavedRecipe struct {
	Owner        int64
	CollectionId uuid.UUID
	RecipeId     string
}

type FeastCollections map[uuid.UUID][]FeastSavedRecipe

func FeastSavedRecipeFromRecord(rec *RecipeRecord) *FeastSavedRecipe {
	splitter := regexp.MustCompile("^(\\d+)-(.*)$")
	parts := splitter.FindAllStringSubmatch(rec.UserAndCollection, -1)
	if parts == nil {
		log.Printf("ERROR Malformed collection id: %s", rec.UserAndCollection)
		return nil
	}
	userId, err := strconv.ParseInt(parts[0][1], 10, 32)
	if err != nil {
		log.Printf("ERROR Malformed user id: %s", parts[0][1])
		return nil
	}
	cid, err := uuid.Parse(parts[0][2])
	if err != nil {
		log.Printf("ERROR Malformed collection id: %s", parts[0][2])
	}

	return &FeastSavedRecipe{
		Owner:        userId,
		CollectionId: cid,
		RecipeId:     rec.ReferencedObjectId,
	}
}

type FeastUsers map[int64][]FeastCollection

// type FeastUser struct {
// 	UserId      int64
// 	Collections []FeastCollection
// }

type CollectionRecord struct {
	UserAndCollection  string
	ReferencedObjectId string
	CollectionType     string
	ContentType        string
	LastModified       string
}

func stringFrom(v types.AttributeValue) string {
	if str, isStr := v.(*types.AttributeValueMemberS); isStr {
		return str.Value
	} else {
		return ""
	}
}

func CollectionRecordFromDynamo(rec *map[string]types.AttributeValue) *CollectionRecord {
	if collectionType, haveCollectionType := (*rec)["collectionType"]; haveCollectionType {
		return &CollectionRecord{
			UserAndCollection:  stringFrom((*rec)["userAndCollection"]),
			ReferencedObjectId: stringFrom((*rec)["referencedObjectId"]),
			CollectionType:     stringFrom(collectionType),
			LastModified:       stringFrom((*rec)["lastModified"]),
		}
	} else {
		return nil
	}
}

type RecipeRecord struct {
	UserAndCollection  string
	ReferencedObjectId string
}

func RecipeRecordFromDynamo(rec *map[string]types.AttributeValue) *RecipeRecord {
	if userAndCollection, haveUserAndCollection := (*rec)["userAndCollection"]; haveUserAndCollection {
		maybeUserString := stringFrom(userAndCollection)
		if strings.Contains(maybeUserString, "-") { //this is a composite ID therefore a recipe record
			return &RecipeRecord{
				UserAndCollection:  maybeUserString,
				ReferencedObjectId: stringFrom((*rec)["referencedObjectId"]),
			}
		} else {
			return nil
		}
	} else {
		return nil
	}
}
