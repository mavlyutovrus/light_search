#include <iostream>
#include <string>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <fstream>
#include <set>
#include <sstream>
#include "server.hpp"
#include <algorithm>
#include "search_engine.hpp"

using std::pair;
using std::string;
using std::vector;
using std::cout;

const string QUERY_PREFIX = "/?";
const string KEYS_DELIM = ";";
const string FIELD_DELIM = ",";


vector<string> splitString(const string& sourceString, const string& delimiter, bool dropEmptyChunks = true) {
	vector<string> out;
	int startPos = 0;
	while (true) {
		int delimPos = sourceString.find(delimiter, startPos);
		if (delimPos == string::npos) {
			break;
		}
		if (delimPos != startPos || !dropEmptyChunks) {
			out.push_back(sourceString.substr(startPos, delimPos - startPos));
		}
		startPos = delimPos + 1;
	}
	if (startPos != sourceString.size() || !dropEmptyChunks) {
		out.push_back(sourceString.substr(startPos, sourceString.size() - startPos));
	}
	return out;
}

unordered_map<string, vector<string> > parseQuery(const http::server::request& req) {
	string query = req.uri.substr(QUERY_PREFIX.size(), req.uri.size() - QUERY_PREFIX.size());
	string chunksDelimiter = "&";
	string keyValDelimiter = "=";
	string valuesDelimiter = ",";
	vector<string> chunks = splitString(query, chunksDelimiter);
	unordered_map<string, vector<string> > keyValuesDict;
	for (auto chunkIt = chunks.begin(); chunkIt != chunks.end(); ++chunkIt) {
		vector<string> keyValue = splitString(*chunkIt, keyValDelimiter);
		if (keyValue.size() == 2) {
			if (keyValuesDict.find(keyValue[0]) == keyValuesDict.end()) {
				keyValuesDict[keyValue[0]] = vector<string>();
			}
			vector<string> values = splitString(keyValue[1], valuesDelimiter);
			keyValuesDict[keyValue[0]].insert(keyValuesDict[keyValue[0]].end(), values.begin(), values.end());
		}
	}
	return keyValuesDict;
}

auto_ptr<TSearchIndex> SearchIndexPtr;


void http::server::request_handler::handle_request(const request& req, reply& rep) {
	{
		bool right_prefix = req.uri.find(QUERY_PREFIX, 0) == 0;
		if (!right_prefix) {
		    rep = reply::stock_reply(reply::bad_request);
		    return;
		}
	}

	unordered_map<string, vector<string> > queryParams = parseQuery(req);
	string dump = "";

	const string QUERY_WORDS_PARAM = "q";
	const string FILTER_OBJECTS_PARAM = "o";
	const string START_PARAM = "start";
	const string LEN_PARAM = "len";

	const int firstResult2Return = atoi(queryParams[START_PARAM][0].c_str());
	const int results2Return = atoi(queryParams[LEN_PARAM][0].c_str());

	if (queryParams.find(QUERY_WORDS_PARAM) != queryParams.end()) {
		const vector<string>& queryKeys = queryParams[QUERY_WORDS_PARAM];
		auto_ptr<unordered_set<TObjectId> > filterObjectsPtr = auto_ptr<unordered_set<TObjectId> >(NULL);
		if (queryParams.find(FILTER_OBJECTS_PARAM) != queryParams.end()) {
			filterObjectsPtr = auto_ptr<unordered_set<TObjectId> > (new unordered_set<TObjectId>());
			for (auto objectIdStrPtr = queryParams[FILTER_OBJECTS_PARAM].begin();
							objectIdStrPtr != queryParams[FILTER_OBJECTS_PARAM].end();
							++objectIdStrPtr) {
				filterObjectsPtr->insert(atoi(objectIdStrPtr->c_str()));
			}
		}
		dump = SearchIndexPtr->ExecuteQuery(queryKeys, firstResult2Return, results2Return, filterObjectsPtr.get());
	}


	string responseString = dump;
	/*
	for (int queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
		vector<TSNP> responses = db.Search(queries[queryIndex]);
		for (int respIndex = 0; respIndex < responses.size(); ++respIndex) {
			responseString.append(boost::lexical_cast<std::string>(responses[respIndex].BpIndex));
			responseString.append(" ");
			responseString.append(boost::lexical_cast<std::string>(responses[respIndex].ID));
			responseString.append(" ");
			responseString.append(responses[respIndex].FirstAllele + " " + responses[respIndex].SecondAllele);
			responseString.append("\n");
		}
		responseString += "=====\n";
	}
	*/

	// Fill out the reply to be sent to the client.
	rep.status = reply::ok;
	rep.content = responseString;
	rep.headers.resize(2);
	rep.headers[0].name = "Content-Length";
	rep.headers[0].value = boost::lexical_cast<std::string>(rep.content.size());
	rep.headers[1].name = "Content-Type";
	rep.headers[1].value = mime_types::extension_to_type("");
}


int main (int argc, char *argv[]) {
	string indexLocation = argv[1];
	try {
		if (argc < 2) {
			std::cerr << "Usage: http_server <index location>\n";
			return 1;
		}
		// Initialise the server.
		SearchIndexPtr = auto_ptr<TSearchIndex>(new TSearchIndex(indexLocation));
		string docRoot = "./";
		string port = "8080";
		http::server::server server("127.0.0.1", port, docRoot);
		server.run();
	} catch (std::exception& e) {
		std::cerr << "exception: " << e.what() << "\n";
	}
    return 0;
}


