const AWS = require('aws-sdk')
const fetch = require("node-fetch");

// AWS.config.update({accessKeyId: '', secretAccessKey: ''})
const s3 = new AWS.S3({region: '<region>'})

var params = {Bucket: '<bucket>', Key: '<key>', Expires: 3600};
var promise = s3.getSignedUrlPromise('getObject', params);

promise.then(function(url) {
  console.log('The URL is', url);
  fetch(url, { mode: 'cors' })
                .then(response => response.text())
                .then(htmlContent => {
                    console.log(htmlContent);
    })
    .catch(err => {
        console.error('Error fetching tabs content html', err);
    });
}, function(err) { 
  console.log(err);
});
