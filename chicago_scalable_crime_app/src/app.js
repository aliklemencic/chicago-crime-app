'use strict';
const http = require('http');
let assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
let hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

hclient.table('aliklemencic_yearly_crimes_by_weather').scan(
	{
		filter: {
			type: "PrefixFilter",
			value: "arson"
		},
		maxVersions: 1
	},
	function (err, cells) {
		console.info(groupByYear("arson", cells));
	}
)

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}

function counterToNumber(c) {
	return Number(Buffer.from(c, 'latin1').readBigInt64BE());
}

function groupByYear(crime, cells) {
	let result = [];
	let yearTotals;
	let lastYear = 0;
	cells.forEach(function (cell) {
		let year = Number(removePrefix(cell['key'], crime));
		if (isNaN(year)) {
			return;
		}
		if(lastYear !== year) {
			if(yearTotals) {
				result.push(yearTotals)
			}
			yearTotals = {'year' : year}
		}
		yearTotals[removePrefix(cell['column'], 'crime:')] = counterToNumber(cell['$'])
		lastYear = year;
	})
	return result;
}


app.use(express.static('public'));
app.get('/crimes.html',function (req, res) {
    const crime=req.query['crime_type'];
	hclient.table('aliklemencic_yearly_crimes_by_weather').scan(
		{
			filter: {
				type: "PrefixFilter",
				value: crime
			},
			maxVersions: 1
		},
		function (err, cells) {
			let template = filesystem.readFileSync("result.mustache").toString();
			let html = mustache.render(template,
									  {yearly_totals: groupByYear(crime, cells),
										  crime_type: crime.replaceAll("_", " ")});
		res.send(html);
	})
});


let kafka = require('kafka-node');
let Producer = kafka.Producer;
let KeyedMessage = kafka.KeyedMessage;
let kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
let kafkaProducer = new Producer(kafkaClient);


app.get('/weather.html', function (req, res) {
	let day = req.query['day'];
	let month = req.query['month'];
	let year = req.query['year'];
	let fog_val = !!(req.query['fog']);
	let rain_val = !!(req.query['rain']);
	let snow_val = !!(req.query['snow']);
	let hail_val = !!(req.query['hail']);
	let thunder_val = !!(req.query['thunder']);
	let tornado_val = !!(req.query['tornado']);
	let report = {
		day : day,
		month : month,
		year : year,
		clear : !fog_val && !rain_val && !snow_val && !hail_val && !thunder_val && !tornado_val,
		fog : fog_val,
		rain : rain_val,
		snow : snow_val,
		hail : hail_val,
		thunder : thunder_val,
		tornado : tornado_val
	};

	kafkaProducer.send([{ topic: 'aliklemencic-weather-reports', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-weather.html');
		});
});


app.get('/crime.html', function (req, res) {
	let day = req.query['day'];
	let month = req.query['month'];
	let year = req.query['year'];
	let crime_type = req.query['crime_type'];
	let report = {
		day : day,
		month : month,
		year : year,
		crime_type : crime_type
	};

	kafkaProducer.send([{ topic: 'aliklemencic-crime-reports', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log(err);
			console.log(report);
			res.redirect('submit-crime.html');
		});
});

	
app.listen(port);
