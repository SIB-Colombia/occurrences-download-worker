# occurrences-download-worker
SIB Dataportal and explorer download worker using kafka. This app work in a node cluster to reduce performance impact.

### How to install
Download required packages
```
npm install
```

###How to run
````
iojs app.js
````

###Note
You must define environment vars for username and password for the email account that will be used for notification.

#### Required environment vars
- DOWNLOAD_GMAIL_USER
- DOWNLOAD_GMAIL_PASSWORD
