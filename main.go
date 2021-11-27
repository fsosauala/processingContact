package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	processedStatus = "processed"
	tableName       = "contactsFredy"
)

type (
	User struct {
		key
		FirstName string `json:"firstName"`
		LastName  string `json:"lastName"`
		Status    string `dynamodbav:"status" json:"status"`
	}
	key struct {
		ID string `dynamodbav:"id" json:"id"`
	}
)

func handleRequest(ctx context.Context, snsEvent events.SNSEvent) error {
	cfg, err := config.LoadDefaultConfig(ctx, func(opts *config.LoadOptions) error {
		opts.Region = os.Getenv("AWS_REGION")
		return nil
	})
	if err != nil {
		log.Printf("error loading dynamo configuration: %v", err)
		return err
	}
	svc := dynamodb.NewFromConfig(cfg)
	for _, record := range snsEvent.Records {
		snsRecord := record.SNS
		err := updateContact(ctx, svc, snsRecord.Message)
		if err != nil {
			log.Printf("error updating contact: %v", err)
			return err
		}
	}
	return nil
}

func main() {
	lambda.Start(handleRequest)
}

func updateContact(ctx context.Context, svc *dynamodb.Client, userString string) error {
	var users []User
	err := json.Unmarshal([]byte(userString), &users)
	if err != nil {
		return err
	}

	if len(users) == 0 {
		log.Printf("no contacts found")
		return nil
	}

	for _, user := range users {
		input, err := userToDynamoType(user)
		if err != nil {
			return err
		}
		_, err = svc.UpdateItem(ctx, input)
		if err != nil {
			return err
		}
	}
	return nil
}

func userToDynamoType(user User) (*dynamodb.UpdateItemInput, error) {
	expr, err := expression.NewBuilder().WithUpdate(
		expression.Set(
			expression.Name("status"),
			expression.Value(processedStatus),
		),
	).Build()
	if err != nil {
		return nil, err
	}
	return &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{
				Value: user.ID,
			},
		},
		UpdateExpression:          expr.Update(),
		ExpressionAttributeValues: expr.Values(),
		ExpressionAttributeNames:  expr.Names(),
	}, nil
}
