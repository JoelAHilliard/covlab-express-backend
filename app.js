const express = require('express');
const { MongoClient } = require('mongodb');
require('dotenv').config(); // Load environment variables from .env file
const cors = require('cors')

const USERNAME = process.env.USERNAME
const PASSWORD = process.env.PASSWORD
// MongoDB connection URL
const mongoUrl = "mongodb://" + encodeURIComponent(USERNAME) + ":" + encodeURIComponent(PASSWORD) + "@covlab.tech:57017/TwitterVisual";

// Initialize Express app
const app = express();
// Middleware to parse JSON data
app.use(express.json());
app.use(cors())
let db;
// Connect to MongoDB
async function connectToMongoDB() {
    try {
      const client = await MongoClient.connect(mongoUrl, { useNewUrlParser: true, useUnifiedTopology: true });
      console.log('MongoDB Connected');
      db = client.db("TwitterVisual"); // Use the correct database name

      return client;
    } catch (err) {
      console.error('Error connecting to MongoDB:', err);
      process.exit(1);
    }
}
// Start the server after connecting to MongoDB
async function startServer() {
    try {
      await connectToMongoDB();
  
      const PORT = process.env.PORT || 3000;
      app.listen(PORT, () => {
        console.log(`Server running on port ${PORT}`);
      });
    } catch (err) {
      console.error('Error starting the server:', err);
      process.exit(1);
    }
}

startServer();

// GET route to fetch all items
app.get('/', async (req, res) => {
    res.json({"msg":"Hello World"});
});
app.get('/latest', async (req, res) => {
    res.json(await latest());
});
app.get('/graphData', async (req, res) => {
    res.json(await casesTweetsGraph());
});
app.get('/graphData1', async (req, res) => {
    res.json(await tweetGraph());
});
app.get('/mapData', async (req, res) => {
    res.json(await getMapData());
});
app.get('/tableData', async (req, res) => {
    res.json(await getTableData());
});
//endpoints and definitions
// 1) /latest | daily data from US Collection | done
// 2) /graphData | transforms DB data into visual data | WIP
// 3) /graphData1 | transforms DB data into visual data | WIP
// 4) /mapData | transforms DB data into map data | done
// 5) /wordCloud | unused | transforms DB data into wordcloud data | WIP
// 6) /tableData | transforms DB data into wordcloud data | WIP

async function latest() {
    try {
      const latest_data = db.collection("daily_real_data_us");
      const sortedLatestData = await latest_data.find().sort({ _id: -1 }).limit(1).toArray();
  
      if (sortedLatestData.length > 0) {
        const newestItem = sortedLatestData[0];
        newestItem["_id"] = String(newestItem["_id"]);
  
        const statsCollection = db.collection("statistics");
        const statsData = await statsCollection.find().toArray();
  
        statsData.forEach(doc => {
          console.log(doc);
        });
      } else {
        console.log("No data found in 'daily_real_data_us' collection.");
      }
    } catch (err) {
      console.error("Error in latest function:", err);
    }
}

async function casesTweetsGraph() {
    const dailyInfectionCollection = db.collection("daily_real_data_us");

    const dailyInfectionData = [];
    
    await dailyInfectionCollection.find().forEach((item)=>{
        dailyInfectionData.push({
            "date":item["date"],
            "new_cases":item["new_cases"],
            "cases_7_average":item["cases_7_average"],
            "cases_14_average":item["cases_14_average"],
            "total_cases":item["total_cases"],
        })
    })


    const tweetData = [];
    
    const dailyTweetCollection = db.collection("daily_positive_tweets_count");

    await dailyTweetCollection.find().forEach((data)=>{
        tweetData.push({
            "date": data['date'],
            "new_tweets": data['new_tweets_count'],
            "tweets_7_average": data['cases_7_average'],
            "tweets_14_average": data['cases_14_average'],
            "total_tweets": data['total_tweets_count'],
            "positive_tweets_ratio":data['positive_tweets_ratio'],
            "weekly_new_cases_per10m":data['weekly_new_cases_per10m']
        })
    })

    let sortedTweets = tweetData.sort((a,b) => {
        return new Date(a.date).valueOf()  - new Date(b.date).valueOf()
    });

    return [dailyInfectionData,sortedTweets];
}

async function tweetGraph(){
    const dailyTweetsCol = db.collection("daily_all_tweets_count");

    const dailyTweetData = [];
    
    await dailyTweetsCol.find().forEach((item)=>{
        dailyTweetData.push({
            "date": item['date'],
            "new_tweets_count": item['new_tweets_count'],
            "total_tweets_count": item['total_tweets_count'],
            "tweets_14_average": item['tweets_14_average'],
            "tweets_7_average": item['tweets_7_average']
        })
    })

    dailyTweetData.sort((a,b)=>{
        return new Date(a.date).valueOf()  - new Date(b.date).valueOf()
    })

    return dailyTweetData;
}

async function getMapData() {
    const mapCollection = db.collection("us_map")
    const stateDataArr = []

    await mapCollection.find().forEach((item)=>{
          stateDataArr.push({
            "state":item['state'],
            "positive":item['total_count']
          })  
    })

    return stateDataArr;
}

async function getTableData(){
    const dailyTweetCollection = db.collection("daily_positive_tweets_count");
    const mapCollection = db.collection("us_map")
    const statsCollection = db.collection("statistics");
    const usDataArr = [];

    await mapCollection.find().sort({ _id: -1 }).forEach(async (item) => {
        const tempStateItem = { data: [] };
      
        for (const [key, value] of Object.entries(item)) {
          if (key === 'state') {
            tempStateItem['state'] = value;
          } else if (!['_id', 'state', 'total_count'].includes(key)) {
            tempStateItem['data'].push([key, value]);
          }
        }
      
        tempStateItem['data'] = tempStateItem['data']
          .sort((a, b) => b[0].localeCompare(a[0]))
          .slice(0, 14)
          .reverse();
      
        usDataArr.push(tempStateItem);
    });

    const latestDailyPositiveTweetsCount = await dailyTweetCollection
      .find()
      .sort({ _id: -1 })
      .toArray();

    const us14DayGraphData = { labels: [], data: [] };
    let count = 0;
    for (const item of latestDailyPositiveTweetsCount) {
      try {
        us14DayGraphData['labels'].push(item['date']);
        us14DayGraphData['data'].push([item['date'], item['cases_14_average']]);
        count += 1;
      } catch (err) {
        console.log("Issue at GRABTABLEDATA, missing key value 'cases_14_average'");
        // console.log(item);
      }
      if (count === 14) break;
    }

    const latestStatisticsData = await statsCollection.find().toArray();
    const stateArr = [];

    for (let counter = 0; counter < latestStatisticsData.length; counter++) {
      const item = latestStatisticsData[counter];
      const stateData = { state: item['type'] };

      // Update the state data with the matching data from US map, using the state value
      const matchingData = usDataArr.find(x => x.state === stateData.state)?.data || [];
      stateData['cases_14_days_change'] = {
        percentage: item['cases_14_days_change'] || 'N/A',
        '14DayData': {
          labels: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
          data: counter > 0 ? matchingData : us14DayGraphData['data']
        }
      };

      try {
        stateData['weekly_new_cases_per10m'] = item['weekly_new_cases_per10m'] || (counter === 0 ? latestDailyPositiveTweetsCount[0]['weekly_new_cases_per10m'] : 'N/A');
      } catch (err) {
        console.log("Key error when getting data, key: weekly_new_cases_per10m");
        stateData['weekly_new_cases_per10m'] = "N/A";
      }

      stateData['cases_7_sum'] = item['cases_7_sum'] || 'N/A';
      stateData['positivity'] = item['positivity'] || 'N/A';
      stateArr.push(stateData);
    }


    return stateArr;
}