var fs = require("fs");
var elasticsearch = require('elasticsearch');
var _ = require('underscore');
var ReadableSearch = require('elasticsearch-streams').ReadableSearch;
var JSONStream = require('JSONStream');
var es = require('event-stream');
var csv = require('fast-csv');
var zlib = require('zlib');
var archiver = require('archiver');
var nodemailer = require('nodemailer');
var moment = require('moment');
var winston = require('winston');

// create reusable transporter object using SMTP transport
var transporter = nodemailer.createTransport({
    service: 'Gmail',
    auth: {
      user: process.env.DOWNLOAD_GMAIL_USER,
      pass: process.env.DOWNLOAD_GMAIL_PASSWORD
    }
});

logger = new (winston.Logger)({
  transports: [
    new (winston.transports.Console)({ level: 'error' }),
    new (winston.transports.File)({ filename: 'logs/download-worker.log' })
    ]
});

var locationSaveFolder = process.env.DOWNLOAD_LOCAL_FOLDER;
var downloadURL = process.env.DOWNLOAD_WEB_FOLDER;

var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client(),
    consumer = new Consumer(
      client,
        [
          { topic: 'occurrencesDownload', partition: 0 }
        ],
        {
          groupId: 'kafka-node-group',
          autoCommit: true
        }
    );

consumer.on('message', processDownload);

function processDownload(message) {
  var request = JSON.parse(message.value);

  // Connect the client, requests will be
  // load-balanced between them using round-robin
  var client = elasticsearch.Client({
    hosts: [
      'localhost:9200'
    ]
  });

  var totalRegisters = 0;
  var offset = 0;
  var limit  = 10000000;
  var page   = 2000;

  var searchExec = function searchExec(from, callback) {
    client.search({
      index: 'sibexplorer',
      from: from + offset,
      size: (offset + from + page) > limit ? (limit - offset - from) : page,
      body: createQuery(request.type, request.query)
    }, callback);
  };

  var fileName = "occurrence-search-"+message.offset+"-"+Date.now();
  var destinationFile = locationSaveFolder+fileName;
  var writableStream = fs.createWriteStream(destinationFile+".csv", {encoding: "utf8"});
  var rs = new ReadableSearch(searchExec);

  var formatStream = csv
    .createWriteStream({headers: true, delimiter: ((process.env.DOWNLOAD_CSV_DELIMITER == 'tab') ? '\t' : ',')})
    .transform(function(obj){
      return {
        "Publicador de datos": obj._source.provider.name,
        "Conjunto de datos": obj._source.resource.name,
        "Dataset Rights": obj._source.resource.rights,
        "Fecha del evento": obj._source.occurrence_date,
        "Código de la institución": obj._source.institution.code,
        "Código de la colección": obj._source.collection.code,
        "Base del registro": obj._source.basis_of_record.name_spanish,
        "Occurrence_id": obj._source.id,
        "Nombre científico": obj._source.canonical,
        "Autor": obj._source.taxon_name_author,
        "Reino": obj._source.taxonomy.kingdom_name,
        "Filo": obj._source.taxonomy.phylum_name,
        "Clase": obj._source.taxonomy.class_name,
        "Orden": obj._source.taxonomy.order_name,
        "Familia": obj._source.taxonomy.family_name,
        "Género": obj._source.taxonomy.genus_name,
        "País": obj._source.country_name,
        "País (calculado)": obj._source.country_name_calculated,
        "Localidad": obj._source.locality,
        "Departamento": obj._source.department_name,
        "Municipio": obj._source.county_name,
        "Páramo": obj._source.paramo_name,
        "Área marítima": obj._source.marine_zone_name,
        "Área protegida": obj._source.protected_area,
        "País del publicador": obj._source.provider.country_name,
        "Latitud": obj._source.location.lat,
        "Longitud": obj._source.location.lon,
        "ID de celda (1 grado)": obj._source.cell_id,
        "ID de celda (0.5 grado)": obj._source.pointfive_cell_id,
        "ID de celda (0.2 grado)": obj._source.pointtwo_cell_id,
        "ID de celda (0.1 grado)": obj._source.centi_cell_id,
        "Profundidad": obj._source.depth_centimeters,
        "Altitud": obj._source.altitude_meters,
        "URL del portal de SIB Colombia": "http://data.sibcolombia.net/occurrences/"+obj._source.id,
        "URL del servicio web de SIB Colombia": "http://data.sibcolombia.net/ws/rest/occurrence/get?key="+obj._source.id
      };
    });

  writableStream.on("finish", function() {
    var archive = archiver('zip');
    var output = fs.createWriteStream(destinationFile+'.zip');

    output.on("finish", function() {
      fs.unlinkSync(destinationFile+'.csv');

      moment.locale('es'); // change the global locale to Spanish
      var stats = fs.statSync(destinationFile+'.zip')
      var fileSizeInBytes = stats["size"]
      //Convert the file size to megabytes (optional)
      var fileSizeInMegabytes = fileSizeInBytes / 1000000.0

      if(totalRegisters == 0) {
        // setup e-mail data with unicode symbols
        var mailOptions = {
          from: 'SIB Colombia <sib+downloads@humboldt.org.co>', // sender address
          to: request.email+' <'+request.email+'>', // list of receivers
          subject: 'Su consulta en la red del SIB esta disponible para descarga', // Subject line
          html: '<p>La solicitud que realizó mediante la plataformaa de SIB Colombia no arrojó ningún registro que cumpla con sus condiciones de búsqueda.</p><p>Le invitamos a cambiar los parámetros de búsqueda en: <a href="http://data.sibcolombia.net">http://data.sibcolombia.net</a> o <a href="http://maps.sibcolombia.net">http://maps.sibcolombia.net</a></p><p>Gracias por usar nuestro sistema de información</p>' // html body
        };
        fs.unlinkSync(destinationFile+'.zip');
      } else {
        // setup e-mail data with unicode symbols
        var mailOptions = {
          from: 'SIB Colombia <sib+downloads@humboldt.org.co>', // sender address
          to: request.email+' <'+request.email+'>', // list of receivers
          subject: 'Su consulta en la red del SIB esta disponible para descarga', // Subject line
          html: '<p>Los datos consultados en la plataforma de SIB Colombia estan disponibles para descarga.</p><p>Puede descargar el archivo en la siguiente dirección: </br></br> <a href="'+downloadURL+fileName+'.zip'+'">'+downloadURL+fileName+'.zip</a><p>Detalles del archivo:</p><p><ul><li>Fecha de generación: '+moment().format('LLLL')+'</li><li>Registros incluidos: '+(totalRegisters-1)+'</li><li>Tamaño de archivo: '+Number((fileSizeInMegabytes).toFixed(2))+' MB</li></ul></p><p>Gracias por usar nuestro sistema de información</p>' // html body
        };
      }

      // send mail with defined transport object
      transporter.sendMail(mailOptions, function(error, info){
        if(error){
          logger.log('Error sending email: ', {email: request.email, timestamp: Date.now(), pid: process.pid});;
        }else{
          logger.info('Email message sent with generated download: ', {email: request.email, totalRegisters: totalRegisters, timestamp: Date.now(), pid: process.pid});
        }
      });
    });
    archive.pipe(output);
    archive.file(destinationFile+'.csv', { name: fileName+'/'+fileName+'.csv' });
    archive.finalize();
  });

  rs.pipe(formatStream).pipe(es.map(function (data, callback) {
    totalRegisters++;
    callback(null,data);
  })).pipe(writableStream);
};

// Returns cell stats with search conditions
function createQuery(queryType, conditions) {
  var qryObj = {
    "_source": [
      "provider.name",
      "resource.name",
      "resource.rights",
      "occurrence_date",
      "institution.code",
      "collection.code",
      "basis_of_record.name_spanish",
      "id",
      "canonical",
      "taxon_name_author",
      "taxonomy.kingdom_name",
      "taxonomy.phylum_name",
      "taxonomy.class_name",
      "taxonomy.order_name",
      "taxonomy.family_name",
      "taxonomy.genus_name",
      "country_name",
      "country_name_calculated",
      "locality",
      "department_name",
      "county_name",
      "paramo_name",
      "marine_zone_name",
      "protected_area",
      "provider.country_name",
      "location.lat",
      "location.lon",
      "cell_id",
      "pointfive_cell_id",
      "pointtwo_cell_id",
      "centi_cell_id",
      "depth_centimeters",
      "altitude_meters"
    ],
    "query": {
      "filtered" : {
        "query" : {
          "bool": {
            "must": []
          }
        },
        "filter": {
          "bool": {
            "must": [
              {
                "missing": {
                  "field": "deleted"
                }
              }
            ]
          }
        }
      }
    }
  };

  if(queryType == "georeferenced") {
    qryObj.query.filtered.filter.bool.must[1] = {
      "term": {
        "geospatial_issue": 0,
      }
    };
    qryObj.query.filtered.filter.bool.must[2] = {
      "exists": {
        "field": "cell_id"
      }
    };
  }

  var andCounter = 0;
  var orCounter = 0;

  if(conditions.scientificNames) {
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.scientificNames, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["canonical.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.commonNames) {
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["path"] = "common_names";
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["query"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["query"]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["query"]["bool"]["should"] = [];
    _.each(conditions.commonNames, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["query"]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["query"]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["nested"]["query"]["bool"]["should"][orCounter]["wildcard"]["common_names.name.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.taxons) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.taxons, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      if(data.textName == "kingdom")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.kingdom_name.exactWords"] = data.textObject.toLowerCase();
      if(data.textName == "phylum")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.phylum_name.exactWords"] = data.textObject.toLowerCase();
      if(data.textName == "class")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.class_name.exactWords"] = data.textObject.toLowerCase();
      if(data.textName == "order")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.order_name.exactWords"] = data.textObject.toLowerCase();
      if(data.textName == "family")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.family_name.exactWords"] = data.textObject.toLowerCase();
      if(data.textName == "genus")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.genus_name.exactWords"] = data.textObject.toLowerCase();
      if(data.textName == "species")
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["taxonomy.species_name.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.countries) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.countries, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["iso_country_code.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.departments) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.departments, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["iso_department_code.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.counties) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.counties, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["iso_county_code.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.paramos) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.paramos, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["paramo_code.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.marineZones) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.marineZones, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["marine_zone_code.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.latitudes) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"] = [];
    _.each(conditions.latitudes, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter] = {};
      if(data.predicate == "eq") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"]["gte"] = data.textObject.toLowerCase();
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"]["lte"] = data.textObject.toLowerCase();
      } else if(data.predicate == "gt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"]["gt"] = data.textObject.toLowerCase();
      } else if(data.predicate == "lt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lat"]["lt"] = data.textObject.toLowerCase();
      }
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.longitudes) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"] = [];
    _.each(conditions.longitudes, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter] = {};
      if(data.predicate == "eq") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"]["gte"] = data.textObject.toLowerCase();
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"]["lte"] = data.textObject.toLowerCase();
      } else if(data.predicate == "gt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"]["gt"] = data.textObject.toLowerCase();
      } else if(data.predicate == "lt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["location.lon"]["lt"] = data.textObject.toLowerCase();
      }
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.altitudes) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"] = [];
    _.each(conditions.altitudes, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter] = {};
      if(data.predicate == "eq") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_meters"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_meters"]["gte"] = data.textObject.toLowerCase();
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_metres"]["lte"] = data.textObject.toLowerCase();
      } else if(data.predicate == "gt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_meters"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_meters"]["gt"] = data.textObject.toLowerCase();
      } else if(data.predicate == "lt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_meters"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["altitude_meters"]["lt"] = data.textObject.toLowerCase();
      }
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.deeps) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"] = [];
    _.each(conditions.deeps, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter] = {};
      if(data.predicate == "eq") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"]["gte"] = data.textObject.toLowerCase();
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"]["lte"] = data.textObject.toLowerCase();
      } else if(data.predicate == "gt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"]["gt"] = data.textObject.toLowerCase();
      } else if(data.predicate == "lt") {
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"] = {};
        qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["must"][orCounter]["range"]["depth_centimeters"]["lt"] = data.textObject.toLowerCase();
      }
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.providers) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.providers, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["provider.name.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.resources) {
    orCounter = 0;
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"] = [];
    _.each(conditions.resources, function(data) {
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"] = {};
      qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["bool"]["should"][orCounter]["wildcard"]["resource.name.exactWords"] = data.textObject.toLowerCase();
      orCounter+=1;
    });
    andCounter+=1;
  }
  if(conditions.poligonalCoordinates || conditions.radialCoordinates) {
    if(conditions.poligonalCoordinates) {
      orCounter = 0;
      qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_polygon"] = {};
      qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_polygon"]["location"] = {};
      qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_polygon"]["location"]["points"] = [];
      _.each(conditions.poligonalCoordinates, function(data) {
        qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_polygon"]["location"]["points"][orCounter] = {"lat": data.lat, "lon": data.lng};
        orCounter+=1;
      });
    }
    if(conditions.radialCoordinates) {
      qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter] = {};
      qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_distance"] = {};
      _.each(conditions.radialCoordinates, function(data) {
        qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_distance"]["distance"] = data.radius + "m";
        qryObj["query"]["filtered"]["filter"]["bool"]["must"][geoPositionCounter]["geo_distance"]["location"] = {"lat": data.lat, "lon": data.lng};
      });
    }
  }
  if(qryObj["query"]["filtered"]["query"]["bool"]["must"].length === 0) {
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter] = {};
    qryObj["query"]["filtered"]["query"]["bool"]["must"][andCounter]["match_all"] = {};
    andCounter+=1;
  }
  return qryObj;
};
