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
- DOWNLOAD_GMAIL_USER: username of the email account used for email notification
- DOWNLOAD_GMAIL_PASSWORD: password of the email account used for email notification
- DOWNLOAD_CSV_DELIMITER: Kind of delimiter used for separated file creation (tab or comma)
- DOWNLOAD_LOCAL_FOLDER: Local folder where the compressed file is saved
- DOWNLOAD_WEB_FOLDER: URL of file download location
