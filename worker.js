//  $$$$$$\ $$$$$$\$$$$$$$\         $$$$$$\          $$\                      $$\      $$\
// $$  __$$\\_$$  _$$  __$$\       $$  __$$\         $$ |                     $$ |     \__|
// $$ /  \__| $$ | $$ |  $$ |      $$ /  \__|$$$$$$\ $$ |$$$$$$\ $$$$$$\$$$$\ $$$$$$$\ $$\ $$$$$$\
// \$$$$$$\   $$ | $$$$$$$\ |      $$ |     $$  __$$\$$ $$  __$$\$$  _$$  _$$\$$  __$$\$$ |\____$$\
//  \____$$\  $$ | $$  __$$\       $$ |     $$ /  $$ $$ $$ /  $$ $$ / $$ / $$ $$ |  $$ $$ |$$$$$$$ |
// $$\   $$ | $$ | $$ |  $$ |      $$ |  $$\$$ |  $$ $$ $$ |  $$ $$ | $$ | $$ $$ |  $$ $$ $$  __$$ |
// \$$$$$$  $$$$$$\$$$$$$$  |      \$$$$$$  \$$$$$$  $$ \$$$$$$  $$ | $$ | $$ $$$$$$$  $$ \$$$$$$$ |
//  \______/\______\_______/        \______/ \______/\__|\______/\__| \__| \__\_______/\__|\_______|
//
//
//

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
    client = new kafka.Client('localhost:2181'),
    consumer = new Consumer(
      client,
        [
          {
            topic: 'occurrencesDownload',
            partition: 0,
            offset: 0
          }
        ],
        {
          groupId: 'kafka-node-group',
          autoCommit: true,
          fromOffset: true
        }
    );

consumer.on('message', processDownload);

function processDownload(message) {
  var request = JSON.parse(message.value);
  //console.log(request);

  // Connect the client, requests will be
  // load-balanced between them using round-robin
  var client = elasticsearch.Client({
    hosts: [
      'localhost:9200'
    ],
    requestTimeout: 60000
  });

  var totalRegisters = 0;
  var fileSizeInBytes = 0;
  var offset = 0;
  var limit  = 10000000;
  var page   = 2000;
  var dataSets = {};

  //console.log( JSON.stringify( createQuery( request.type, getUrlVars(request.queryUrlParameters) ) ) );

  var searchExec = function searchExec(from, callback) {
    client.search({
      index: 'sibdataportal',
      type: 'occurrence',
      from: from + offset,
      size: (offset + from + page) > limit ? (limit - offset - from) : page,
      body: createQuery(request.type, getUrlVars(request.queryUrlParameters))
    }, callback);
  };

  var fileName = "occurrence-search-"+message.offset+"-"+Date.now();
  var destinationFile = locationSaveFolder+'/'+fileName;
  var writableStream = fs.createWriteStream(destinationFile+".txt", {encoding: "utf8"});
  var rs = new ReadableSearch(searchExec);

  var formatStream = csv
    .createWriteStream({headers: true, delimiter: ((process.env.DOWNLOAD_CSV_DELIMITER == 'tab') ? '\t' : ',')})
    .transform(function(obj){
      return {
        "Occurrence_id": (typeof(obj._source['occurrenceid']) != "undefined") ? obj._source['occurrenceid'][0] : "",
        "Publicador de datos": (typeof(obj._source['provider.name']) != "undefined") ? obj.fi_sourceelds['provider.name'][0] : "",
        "Base del registro": (typeof(obj._source['basis_of_record.name']) != "undefined") ? obj._source['basis_of_record.name'][0] : "",
        "Conjunto de datos": (typeof(obj._source['resource.name']) != "undefined") ? obj._source['resource.name'][0] : "",
        "Fecha de publicación": (typeof(obj._source['resource.publication_date']) != "undefined") ? obj._source['resource.publication_date'][0] : "",
        "Nombre científico": (typeof(obj._source['canonical']) != "undefined") ? obj._source['canonical'][0] : "",
        "Reino": (typeof(obj._source['taxonomy.kingdom_name']) != "undefined") ? obj._source['taxonomy.kingdom_name'][0] : "",
        "Filo": (typeof(obj._source['taxonomy.phylum_name']) != "undefined") ? obj._source['taxonomy.phylum_name'][0] : "",
        "Clase": (typeof(obj._source['taxonomy.class_name']) != "undefined") ? obj._source['taxonomy.class_name'][0] : "",
        "Orden": (typeof(obj._source['taxonomy.order_name']) != "undefined") ? obj._source['taxonomy.order_name'][0] : "",
        "Familia": (typeof(obj._source['taxonomy.family_name']) != "undefined") ? obj._source['taxonomy.family_name'][0] : "",
        "Género": (typeof(obj._source['taxonomy.genus_name']) != "undefined") ? obj._source['taxonomy.genus_name'][0] : "",
        "Especie": (typeof(obj._source['taxonomy.species_name']) != "undefined") ? obj._source['taxonomy.species_name'][0] : "",
        "Epíteto específico": (typeof(obj._source['taxonomy.specific_epithet']) != "undefined") ? obj._source['taxonomy.specific_epithet'][0] : "",
        "Epíteto infraespecífico": (typeof(obj._source['taxonomy.infraspecific_epithet']) != "undefined") ? obj._source['taxonomy.infraspecific_epithet'][0] : "",
        "Categoría taxonómica": (typeof(obj._source['taxon_rank']) != "undefined") ? obj._source['taxon_rank'][0] : "",
        "País": (typeof(obj._source['country_name']) != "undefined") ? obj._source['country_name'][0] : "",
        "Departamento": (typeof(obj._source['department_name']) != "undefined") ? obj._source['department_name'][0] : "",
        "Municipio": (typeof(obj._source['county_name']) != "undefined") ? obj._source['county_name'][0] : "",
        "Localidad": (typeof(obj._source['locality']) != "undefined") ? obj._source['locality'][0] : "",
        "Latitud": (typeof(obj._source['location.lat']) != "undefined") ? obj._source['location.lat'][0] : "",
        "Longitud": (typeof(obj._source['location.lon']) != "undefined") ? obj._source['location.lon'][0] : "",
        "Elevación mínima": (typeof(obj._source['minimum_elevation']) != "undefined") ? obj._source['minimum_elevation'][0] : "",
        "Elevación máxima": (typeof(obj._source['maximum_elevation']) != "undefined") ? obj._source['maximum_elevation'][0] : "",
        "Elevación literal": (typeof(obj._source['verbatim_elevation']) != "undefined") ? obj._source['verbatim_elevation'][0] : "",
        "Hábitat": (typeof(obj._source['habitat']) != "undefined") ? obj._source['habitat'][0] : "",
        "Fecha del evento": (typeof(obj._source['eventdate_start']) != "undefined") ? obj._source['eventdate_start'][0] : "",
        "Código de la institución": (typeof(obj._source['institution.code']) != "undefined") ? obj._source['institution.code'][0] : "",
        "Código de la colección": (typeof(obj._source['collection.code']) != "undefined") ? obj._source['collection.code'][0] : "",
        "Nombre de la colección": (typeof(obj._source['collection.name']) != "undefined") ? obj._source['collection.name'][0] : "",
        "Número de catálogo": (typeof(obj._source['catalog.number']) != "undefined") ? obj._source['catalog.number'][0] : "",
        "URL del portal de SIB Colombia": "http://datos.biodiversidad.co"
      };
    });

  writableStream.on("finish", function() {
    var rightsText = "Por favor respete los derechos y permisos declarados para cada conjunto de datos en esta descarga:\n";
    for (var property in dataSets) {
      if(dataSets[property].rights) {
        rightsText = rightsText + "\nConjunto de datos: "+property+", http://data.sibcolombia.net/datasets/resource/"+dataSets[property].id+"\n"+"Licencia de uso: "+dataSets[property].rights+"\n";
      }
    }
    var citationText = "";
    for (var property in dataSets) {
      if(dataSets[property].citation) {
        citationText = citationText + "\nConjunto de datos: "+property+"\n\nPor favor citar estos datos de la siguiente manera:: "+dataSets[property].citation+"\n\n\n";
      }
    }
    var archive = archiver('zip');
    var output = fs.createWriteStream(destinationFile+'.zip');

    output.on("finish", function() {
      fs.unlinkSync(destinationFile+'.txt');

      moment.locale('es'); // change the global locale to Spanish
      var stats = fs.statSync(destinationFile+'.zip')
      if(typeof stats["size"] !== "undefined") {
        fileSizeInBytes = stats["size"];
      }
      //Convert the file size to megabytes (optional)
      var fileSizeInMegabytes = fileSizeInBytes / 1000000.0

      logger.info('Download request success', {email: request.email, totalRegisters: totalRegisters, requestDate: request.date, processFinishDate: Date.now()/1000, pid: process.pid, fileSize: Number((fileSizeInBytes / 1000.0).toFixed(2)), type: request.type, query: request.query, reason: request.reason, remoteip: request.remoteip});

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
          html: '<p>Los datos consultados en la plataforma de SIB Colombia están disponibles para descarga.</p><p>Puede descargar el archivo en la siguiente dirección: </br></br> <a href="'+downloadURL+'/'+fileName+'.zip'+'">'+downloadURL+'/'+fileName+'.zip</a><p>Detalles del archivo:</p><p><ul><li>Fecha de generación: '+moment().format('LLLL')+'</li><li>Registros incluidos: '+totalRegisters+'</li><li>Tamaño de archivo: '+Number((fileSizeInMegabytes).toFixed(2))+' MB</li></ul></p><p><em><strong>Nota: </strong>El archivo con los registros solicitados esta con extensión txt y codificación UTF8, tenga presente que si desea importar los datos con excel debe importar el archivo con codificación UTF8 ya que de forma predeterminada excel importa los archivos con formato Windows ANSI.</em></p><p>Gracias por usar nuestro sistema de información</p>' // html body
        };
      }

      // send mail with defined transport object
      transporter.sendMail(mailOptions, function(error, info){
        if(error){
          logger.warn('Error sending email', {email: request.email, totalRegisters: totalRegisters, requestDate: request.date, processFinishDate: Date.now()/1000, pid: process.pid, fileSize: Number((fileSizeInBytes / 1000.0).toFixed(2)), type: request.type, query: request.query, reason: request.reason, remoteip: request.remoteip});
        }
      });
    });

    output.on("error", function(error) {
      logger.error('Download request error', {email: request.email, totalRegisters: totalRegisters, requestDate: request.date, processFinishDate: Date.now()/1000, pid: process.pid, fileSize: Number((fileSizeInBytes / 1000.0).toFixed(2)), type: request.type, query: request.query, reason: request.reason, error: error, remoteip: request.remoteip});
    });

    archive.pipe(output);
    archive.append(rightsText, { name: fileName+'/rights.txt' });
    archive.append(citationText, { name: fileName+'/citation.txt' });
    archive.file(destinationFile+'.txt', { name: fileName+'/'+fileName+'.txt' });
    archive.finalize();
  });

  rs.on("data", function(data) {
    totalRegisters++;
    if(data._source['resource.name']) {
      dataSets[data._source['resource.name'][0]] = {
        "rights": (typeof(data._source['resource.intellectual_rights']) != "undefined") ? data._source['resource.intellectual_rights'][0] : "",
        "id": (typeof(data._source['resource.id']) != "undefined") ? data._source['resource.id'][0] : "",
        "citation": (typeof(data._source['resource.citation']) != "undefined") ? data._source['resource.citation'][0] : ""
      }
    }
  });

  rs.on("error", function(error) {
    logger.error('Download request error: ', {email: request.email, totalRegisters: totalRegisters, requestDate: request.date, processFinishDate: Date.now()/1000, pid: process.pid, fileSize: Number((fileSizeInBytes / 1000.0).toFixed(2)), type: request.type, query: request.query, reason: request.reason, error: error, remoteip: request.remoteip});
  });

  rs.pipe(formatStream).pipe(writableStream);
};

function getUrlVars(url) {
  var hash;
  var myJson = {};
  var hashes = url.slice(url.indexOf('?') + 1).split('&');
  for (var i = 0; i < hashes.length; i++) {
    hash = hashes[i].split('=');
    hash[0] = hash[0].replace('[','').replace(']','');
    if(myJson[hash[0]]) {
      myJson[hash[0]].push(hash[1]);
    } else {
      myJson[hash[0]] = [];
      myJson[hash[0]].push(hash[1]);
    }
  }
  return myJson;
}

// Returns cell stats with search conditions
function createQuery(queryType, conditions) {
  var countAndQueries = 1;

  //console.log(conditions);

  var query = {
    "_source": [
      "provider.name",
      "resource.name",
      "resource.intellectual_rights",
      "resource.id",
      "resource.citation",
      "resource.publication_date",
      "eventdate_start",
      "institution.code",
      "collection.code",
      "collection.name",
      "basis_of_record.name",
      "id",
      "occurrenceid",
      "canonical",
      "taxonomy.kingdom_name",
      "taxonomy.phylum_name",
      "taxonomy.class_name",
      "taxonomy.order_name",
      "taxonomy.family_name",
      "taxonomy.genus_name",
      "taxonomy.species_name",
      "taxonomy.specific_epithet",
      "taxonomy.infraspecific_epithet",
      "country_name",
      "department_name",
      "county_name",
      "locality",
      "taxon_rank",
      "location.lat",
      "location.lon",
      "minimum_elevation",
      "maximum_elevation",
      "verbatim_elevation",
      "catalog.number",
      "habitat"
    ],
    query: {
      bool: {
        must: [
          {
            query_string: {
              query: '*'
            }
          }
        ]
      }
    }
  };

  // If wildcard queries
  if(conditions.scientificName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.scientificName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'canonical.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.kingdomName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.kingdomName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.kingdom_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.phylumName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.phylumName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.phylum_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.className) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.className, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.class_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.orderName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.orderName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.order_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.familyName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.familyName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.family_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.genusName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.genusName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.genus_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.speciesName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.speciesName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.species_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.specificEpithetName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.specificEpithetName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.specific_epithet.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.infraspecificEpithetName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.infraspecificEpithetName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'taxonomy.infraspecific_epithet.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.providerName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.providerName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'provider.name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.resourceName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.resourceName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'resource.name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.collectionName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.collectionName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'collection.name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.institutionCode) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.institutionCode, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'institution.code.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.countryName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.countryName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'country_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.departmentName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.departmentName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'department_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.countyName) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.countyName, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'county_name.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.habitat) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.habitat, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'habitat.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }
  if (conditions.basisOfRecord) {
    query.query.bool.must[countAndQueries] = {
      bool: {
        should: []
      }
    };
    var counter = 0;
    _.each(conditions.basisOfRecord, function(data) {
      query.query.bool.must[countAndQueries].bool.should[counter] = {
        wildcard: {
          'basis_of_record.exactWords': '*'+data.toLowerCase()+'*'
        }
      };
      counter += 1;
    });
    countAndQueries += 1;
  }

  return query;
};
