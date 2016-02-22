import java.util.ArrayList;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.kms.model.AliasListEntry;

public class DynamoDBClient {

	static DynamoDB dynamoDB;
	
	public static void main(String[] args) throws InterruptedException {

		BasicAWSCredentials credentials = new BasicAWSCredentials("test", "test");
		AmazonDynamoDBClient dbClient = new AmazonDynamoDBClient(credentials);

		dbClient.setEndpoint("http://localhost:8000");

		dynamoDB = new DynamoDB(dbClient);

		TableCollection<ListTablesResult> tables = dynamoDB.listTables();

		ArrayList<String> tableList = new ArrayList<>();
		for (Table table : tables) {
			System.out.println(table.getTableName());
			tableList.add(table.getTableName());
		}

		if (tableList.contains("tweet")) {
			System.out.println("tweet table is exist");
			putItem();
			System.exit(0);
		}

		// TODO 初期化用のバッチみたいにしておく
		String key = "tweetId";
		String tableName = "tweet";
		ArrayList<AttributeDefinition> definitions = new ArrayList<>();
		definitions.add(new AttributeDefinition().withAttributeName(key).withAttributeType("S"));

		ArrayList<KeySchemaElement> schemaElements = new ArrayList<>();
		schemaElements.add(new KeySchemaElement().withAttributeName(key).withKeyType(KeyType.HASH));

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withKeySchema(schemaElements).withAttributeDefinitions(definitions).withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
		
		Table table = dynamoDB.createTable(createTableRequest);
		table.waitForActive();
		
		System.out.println("tweet table is created");

	}

	private static void putItem() {
		
		String tableName = "tweet";
		Table table = dynamoDB.getTable(tableName);
		
		Item item = new Item().withPrimaryKey("tweetId", "1").withString("threadId", "1").withJSON("tweet", json);
		
		table.putItem(item);
		
		Item getItem = table.getItem("tweetId","1");
		
		System.out.println(getItem.toJSONPretty());
	}

	
	static String json  = "{"+
"  \"coordinates\": null,"+
"  \"favorited\": false,"+
"  \"truncated\": false,"+
"  \"created_at\": \"Wed Jun 06 20:07:10 +0000 2012\","+
"  \"id_str\": \"210462857140252672\","+
"  \"entities\": {"+
"    \"urls\": ["+
"      {"+
"        \"expanded_url\": \"https://dev.twitter.com/terms/display-guidelines\","+
"        \"url\": \"https://t.co/Ed4omjYs\","+
"        \"indices\": ["+
"          76,"+
"          97"+
"        ],"+
"        \"display_url\": \"dev.twitter.com/terms/display-\u2026\""+
"      }"+
"    ],"+
"    \"hashtags\": ["+
"      {"+
"        \"text\": \"Twitterbird\","+
"        \"indices\": ["+
"          19,"+
"          31"+
"        ]"+
"      }"+
"    ],"+
"    \"user_mentions\": ["+
" "+
"    ]"+
"  },"+
"  \"in_reply_to_user_id_str\": null,"+
"  \"contributors\": ["+
"    14927800"+
"  ],"+
"  \"text\": \"Along with our new #Twitterbird, we've also updated our Display Guidelines: https://t.co/Ed4omjYs  ^JC\","+
"  \"retweet_count\": 66,"+
"  \"in_reply_to_status_id_str\": null,"+
"  \"id\": 210462857140252672,"+
"  \"geo\": null,"+
"  \"retweeted\": true,"+
"  \"possibly_sensitive\": false,"+
"  \"in_reply_to_user_id\": null,"+
"  \"place\": null,"+
"  \"user\": {"+
"    \"profile_sidebar_fill_color\": \"DDEEF6\","+
"    \"profile_sidebar_border_color\": \"C0DEED\","+
"    \"profile_background_tile\": false,"+
"    \"name\": \"Twitter API\","+
"    \"profile_image_url\": \"http://a0.twimg.com/profile_images/2284174872/7df3h38zabcvjylnyfe3_normal.png\","+
"    \"created_at\": \"Wed May 23 06:01:13 +0000 2007\","+
"    \"location\": \"San Francisco, CA\","+
"    \"follow_request_sent\": false,"+
"    \"profile_link_color\": \"0084B4\","+
"    \"is_translator\": false,"+
"    \"id_str\": \"6253282\","+
"    \"entities\": {"+
"      \"url\": {"+
"        \"urls\": ["+
"          {"+
"            \"expanded_url\": null,"+
"            \"url\": \"http://dev.twitter.com\","+
"            \"indices\": ["+
"              0,"+
"              22"+
"            ]"+
"          }"+
"        ]"+
"      },"+
"      \"description\": {"+
"        \"urls\": ["+
" "+
"        ]"+
"      }"+
"    },"+
"    \"default_profile\": true,"+
"    \"contributors_enabled\": true,"+
"    \"favourites_count\": 24,"+
"    \"url\": \"http://dev.twitter.com\","+
"    \"profile_image_url_https\": \"https://si0.twimg.com/profile_images/2284174872/7df3h38zabcvjylnyfe3_normal.png\","+
"    \"utc_offset\": -28800,"+
"    \"id\": 6253282,"+
"    \"profile_use_background_image\": true,"+
"    \"listed_count\": 10774,"+
"    \"profile_text_color\": \"333333\","+
"    \"lang\": \"en\","+
"    \"followers_count\": 1212963,"+
"    \"protected\": false,"+
"    \"notifications\": null,"+
"    \"profile_background_image_url_https\": \"https://si0.twimg.com/images/themes/theme1/bg.png\","+
"    \"profile_background_color\": \"C0DEED\","+
"    \"verified\": true,"+
"    \"geo_enabled\": true,"+
"    \"time_zone\": \"Pacific Time (US & Canada)\","+
"    \"description\": \"The Real Twitter API. I tweet about API changes, service issues and happily answer questions about Twitter and our API. Don't get an answer? It's on my website.\","+
"    \"default_profile_image\": false,"+
"    \"profile_background_image_url\": \"http://a0.twimg.com/images/themes/theme1/bg.png\","+
"    \"statuses_count\": 3333,"+
"    \"friends_count\": 31,"+
"    \"following\": true,"+
"    \"show_all_inline_media\": false,"+
"    \"screen_name\": \"twitterapi\""+
"  },"+
"  \"in_reply_to_screen_name\": null,"+
"  \"source\": \"web\","+
"  \"in_reply_to_status_id\": null"+
"}";
}
