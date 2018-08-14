// database-connection.js
// Where we connect to Airtable and handle requests from server.js

const Airtable = require('airtable');

const base = new Airtable({
  apiKey: process.env.AIRTABLE_API_KEY,
}).base(process.env.AIRTABLE_BASE_ID);

// ^ Configure Airtable using values in 🗝.env

const tableReviews = 'Reviews';
const tableAmenities = 'Amenities'

// ^ These are the tables we'll be reading from Airtable

const Bottleneck = require('bottleneck');
const rateLimiter = new Bottleneck({
  minTime: 1050 / 5
}) // ~5 requests per second

// ^ Bottleneck, instanced as rateLimiter, allows us to conform to rate limits specified by Airtable's API

//    Failure to comply with the Airtable rate limit locks down its API for 30 seconds:
//    https://airtable.com/api

const cache = require('./caching');

// ^ caching.js reads and writes local files 

function sendResultWithResponse(result, response) {
  response.status(200).end(JSON.stringify(result));
}

function cachePathForRequest(request) {
  return '.newcache' + request.path + '.json';  
}
                          
function handleMilkspotsRequest(record) {  
  console.log(record)
  return {
    id: record.id,
    name: record.get('name'),
    address: record.get('address'),
    lat: record.get('lat'),
    lng: record.get('lng'),
    directions: record.get('directions'),
    cross_streets: record.get('cross_streets'),
    website: record.get('website'),
    location_details: record.get('location_details'),
    category: record.get('category'),
    amenities: record.get('Amenities'),
    area: record.get('area'),
    verified: record.get('verified'),
    reviews: record.get('Reviews'),
  }
}

function handleReviewsRequest(record) {
  return {
    id: record.id,
    timestamp: record.get('Timestamp'),
    milkspot_id: record.get('Milkspot'),
    recommend: record.get('Recommend?'),
  }
}

function handleAmenitiesRequest(record) {
  return {
    id: record.id,
    name: record.get('Name'),
    image: record.get('Glitch Image URL'),
  }
}

module.exports = {

  fetchAirtableData(request, response, tableName, viewName) {
    var cachePath = cachePathForRequest(request);
    
    var cachedResult = cache.readCacheWithPath(cachePath);

    if (cachedResult != null) {
      console.log("Cache hit. Returning cached result for " + request.path);
      sendResultWithResponse(cachedResult, response);
    }
    else {
      
      console.log("Cache miss. Loading from Airtable for " + request.path);

      var pageNumber = 0;

      rateLimiter.wrap(base(tableName).select({
        view: viewName,
        pageSize: 100 //This page size is unnecessarily small, for demonstration purposes.
                    //You should probably use the default of 100 in your own code.
      }).eachPage(function page(records, fetchNextPage) {
        if (pageNumber == request.params.page) {

          var results = [];

          records.forEach(function(record) {
            console.log(record.fields);
            if (tableName == 'Milkspots') {
              var result = handleMilkspotsRequest(record)
            } else if (tableName == 'Reviews') {
              var result = handleReviewsRequest(record)
            } else if (tableName == 'Amenities') {
              var result = handleAmenitiesRequest(record)
            } else {
              console.log(`${tableName} not found`)
            }
            console.log(result);
            results.push(result);
            
          });

          cache.writeCacheWithPath(cachePath, results);
          console.log("Returning records");
          sendResultWithResponse(results, response);

        } else {
          pageNumber++;
          fetchNextPage();
        }

      }, function done(error) {
          sendResultWithResponse([], response);
      }));

    }
            
}

}