#include <iostream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <algorithm>
#include <stdio.h>
#include <time.h> 
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <sstream>
#include <math.h>
#include <queue>
#include <math.h>
#include <chrono>

using namespace std;

const unsigned short OBJ_BITS = 20;
const unsigned short SEGMENT_POS_BITS = 6;
const unsigned short WEIGHT_BITS = 2;
const unsigned short NON_SEGMENT_BITS = SEGMENT_POS_BITS + WEIGHT_BITS + OBJ_BITS;

const int MAX_SEGMENTS_PER_OBJECT = 10;

const char* KEY2OFFSETS_FILE = "main_index_keys.db.txt";
const char* OCCURENCES_FILE = "main_index_values.pickle";
const int MAX_WORDS4QUERY = 10;
const int MAX_KEYS2CONSIDER = 5;
const double CRUDE_FILTER_TRIM_PROPORTION = 0.5;
const int MAX_OCCURENCES2RETURN = 10000;


class TSearchIndex {
private:
typedef unsigned long TSegmentId;
typedef unsigned short TPosition;
typedef unsigned short TWeight;
typedef unsigned short TPositionAndWeight;
typedef unsigned short TWordIndex;
typedef unsigned int TObjectId;

typedef std::pair<TPositionAndWeight, TWordIndex> TPosWeightWithWIndex;

typedef unsigned long long TOccurence;
typedef vector<TOccurence> TOccurences;

typedef pair<unsigned int, unsigned long long> TFreqAndLocation;
typedef string TKey;

    auto_ptr<unordered_map<TKey, TFreqAndLocation> > Key2FreqAndLocationPtr;
    FILE* OccurencesFilePtr;

struct TSegmentMatches {
    TSegmentId Segment;
    TObjectId Object;
    vector<TPosWeightWithWIndex> SegmentOccurences;
    TSegmentMatches(TSegmentId segment, TObjectId object)
	    : Segment(segment)
        , Object(object)
    {
    }
};

static char* strdup(const char* org) {
    if (org == NULL) return NULL;
    char* newstr = (char*)malloc(strlen(org)+1);
    newstr[strlen(org)] = '\0';
    char* p;
    if (newstr == NULL) return NULL;
    p = newstr;
    while(*org) *p++ = *org++; /* copy the string. */
    return newstr;
}


    //typedef std::chrono::time_point<std::chrono::system_clock> TTime;
    //static inline TTime GetTime() {
    //    return std::chrono::system_clock::now();
    //}

    //static inline double GetElapsedInSeconds(TTime start, TTime end) {
    //    std::chrono::duration<double> elapsed = end - start;
    //    return elapsed.count();
    //}

    static inline TSegmentId GetSegmentId(TOccurence occurence) {
        return occurence >> NON_SEGMENT_BITS;
    }
    static inline TObjectId GetObjectId(TOccurence occurence) {
    	return (occurence - ((occurence >> NON_SEGMENT_BITS) << NON_SEGMENT_BITS)) >> (SEGMENT_POS_BITS + WEIGHT_BITS);
    }
    static inline unsigned short int  GetSegmentPosition(TOccurence occurence) {
        return (occurence - ((occurence >> (SEGMENT_POS_BITS + WEIGHT_BITS)) << (SEGMENT_POS_BITS + WEIGHT_BITS))) >> WEIGHT_BITS;
    }
    static inline unsigned short int GetWeight(TOccurence occurence) {
        return (occurence - ((occurence >> WEIGHT_BITS) << WEIGHT_BITS));
    }
    int TmpBufferPositions[(1 << SEGMENT_POS_BITS) + 1];
    int TmpBufferPosition2KeyIndex[(1 << SEGMENT_POS_BITS) + 1];
    int TmpBufferCountsInside[MAX_WORDS4QUERY];
public:
	TSearchIndex(const char* indexLocation)
				: Key2FreqAndLocationPtr(new unordered_map<TKey, TFreqAndLocation>())
	{
		{//upload key -> (freq, offset)
		    ifstream file(string(indexLocation) + "/" + string(KEY2OFFSETS_FILE));
		    if (!file.is_open()) {
		    	return;
		    }
		    string line;
		    while (getline(file, line)) {
		    	if (!line.size() ) {
		    		continue;
		    	}
		    	stringstream linestream(line);
		    	string key;
		    	int freq;
		    	unsigned long long offset;
		    	linestream >> key;
		    	linestream >> freq;
		    	linestream >> offset;
		    	(*Key2FreqAndLocationPtr)[key] = TFreqAndLocation(freq, offset);
		    }
		}
		//occurences file
		OccurencesFilePtr = fopen((string(indexLocation) + "/" + string(OCCURENCES_FILE)).c_str(), "rb");
	}

	~TSearchIndex() {
		if (OccurencesFilePtr != NULL) {
			fclose(OccurencesFilePtr);
		}
	}

private:
	static inline double Freq2IdfFreq(unsigned long long freq) {
		return 1.0 / log(freq + 2.0);
	}

	inline double SegmentMatchSimpleWeight(const TSegmentMatches& match,
						       const unsigned long long* keyIndex2freq) const {
	        unsigned long long seen = 0;
	    	double weight = 0.0;
    		for (int occIndex = 0; occIndex < (int)match.SegmentOccurences.size(); ++occIndex) {
		    int keyIndex = match.SegmentOccurences.at(occIndex).second;
		    if (!(seen & (1 << keyIndex))) {
			seen += (1 << keyIndex);
			weight += Freq2IdfFreq(keyIndex2freq[keyIndex]);
		    }
		}
		return weight;
	}

	
	inline double SegmentMatchSimpleWeight1(const TSegmentMatches& match, const unsigned long long* keyIndex2freq) {
	        int bufferEnd = 0; 
		int uniqTokensCount = 0;
	        unsigned long long seen = 0;
		double weight = 0;
		for (auto occIt = match.SegmentOccurences.begin(); occIt != match.SegmentOccurences.end(); ++occIt) {
			const TPosWeightWithWIndex& occurence = *occIt;
			int keyIndex = occurence.second;
			int position = occurence.first >> WEIGHT_BITS;
			TmpBufferPosition2KeyIndex[position] = keyIndex;
			TmpBufferPositions[bufferEnd] = position;
			TmpBufferCountsInside[keyIndex] = 0;
		        ++bufferEnd;	
		        if (!(seen & (1 << keyIndex))) {
			    seen += (1 << keyIndex);
			    ++uniqTokensCount;
			    weight += Freq2IdfFreq(keyIndex2freq[keyIndex]);
		        }
		}
		sort(&TmpBufferPositions[0], &TmpBufferPositions[0] + bufferEnd);
		int shortest_span_len = -1;
		{

			int start = 0;
			int end = 1;
			shortest_span_len = -1;
			++TmpBufferCountsInside[TmpBufferPosition2KeyIndex[TmpBufferPositions[start]]];
			int uniqInside = 1;
			while (start < bufferEnd) {
				while (end < bufferEnd && uniqInside < uniqTokensCount) {
					if (TmpBufferCountsInside[TmpBufferPosition2KeyIndex[TmpBufferPositions[end]]] == 0) {
						++uniqInside;
					}
					++TmpBufferCountsInside[TmpBufferPosition2KeyIndex[TmpBufferPositions[end]]];
					++end;
				}
				if (uniqInside < uniqTokensCount) {
					break;
				}
				int span_len = TmpBufferPositions[end - 1] - TmpBufferPositions[start] + 1;
				if (shortest_span_len == -1 || span_len < shortest_span_len) {
					shortest_span_len = span_len;
				}
				if (TmpBufferCountsInside[TmpBufferPosition2KeyIndex[TmpBufferPositions[start]]] == 1) {
					--uniqInside;
				}
				--TmpBufferCountsInside[TmpBufferPosition2KeyIndex[TmpBufferPositions[start]]];
				++start;
			}
		}
		return weight + (double)uniqTokensCount / (1000 * shortest_span_len);
	}

	TSegmentMatches SelectShortestSpan(TSegmentMatches& match) {
		int uniqTokensCount = 0;
		int maxFreqs[MAX_WORDS4QUERY]{0};
		for (auto occIt = match.SegmentOccurences.begin(); occIt != match.SegmentOccurences.end(); ++occIt) {
			if (maxFreqs[occIt->second] == 0) {
				++uniqTokensCount;
			}
			++maxFreqs[occIt->second];
		}
		sort(match.SegmentOccurences.begin(), match.SegmentOccurences.end());
		int start = 0;
		int end = 1;
		pair<int, int> shortest_span = pair<int, int>(start, end);
		int shortest_span_len = -1;
		int countsInside[MAX_WORDS4QUERY]{0};
		++countsInside[match.SegmentOccurences[start].second];
		int uniqInside = 1;
		while (start < (int)match.SegmentOccurences.size()) {
			while (end < (int)match.SegmentOccurences.size() && uniqInside < uniqTokensCount) {
				if (countsInside[match.SegmentOccurences[end].second] == 0) {
					++uniqInside;
				}
				++countsInside[match.SegmentOccurences[end].second];
				++end;
			}
			if (uniqInside < uniqTokensCount) {
				break;
			}
			int span_len =  (match.SegmentOccurences[end - 1].first >> WEIGHT_BITS) - (match.SegmentOccurences[start].first >> WEIGHT_BITS) + 1;
			if (shortest_span_len == -1 || span_len < shortest_span_len) {
				shortest_span = pair<int, int>(start, end);
				shortest_span_len = span_len;
			}
			if (countsInside[match.SegmentOccurences[start].second] == 1) {
				--uniqInside;
			}
			--countsInside[match.SegmentOccurences[start].second];
			++start;
		}
		TSegmentMatches shortestMatch(match.Segment, match.Object);
		shortestMatch.SegmentOccurences = vector<TPosWeightWithWIndex>(match.SegmentOccurences.begin() + shortest_span.first,
																	   match.SegmentOccurences.begin() + shortest_span.second);
		return shortestMatch;
	}

	pair<int, double> CalcRelevance(TSegmentMatches& match, const vector<TKey>& tokens, const unsigned long long* token2freq) {
		sort(match.SegmentOccurences.begin(), match.SegmentOccurences.end());
		double max_case_weight = 0;
		for (auto occIt = match.SegmentOccurences.begin(); occIt != match.SegmentOccurences.end(); ++occIt) {
			max_case_weight = max(max_case_weight, (double)(occIt->first - ((occIt->first >> WEIGHT_BITS) << WEIGHT_BITS)));
		}
		double norm_match_weight = 0;
		int matched_words_count = 0;
		int tokensInQuery = 0;
		{
			double match_weight = 0;
			int seen[MAX_WORDS4QUERY]{0};
			for (auto occIt = match.SegmentOccurences.begin(); occIt != match.SegmentOccurences.end(); ++occIt) {
				if (seen[occIt->second] == 0) {
					match_weight += Freq2IdfFreq(token2freq[occIt->second]);
					++matched_words_count;
					++seen[occIt->second];
				}
			}
			double query_weight = 0;
			for (int keyIndex = 0; keyIndex < MAX_WORDS4QUERY; ++keyIndex) {
				if (token2freq[keyIndex] > 0) {
					query_weight += Freq2IdfFreq(token2freq[keyIndex]);
					++tokensInQuery;
				}
			}
			norm_match_weight = (match_weight + 0.0000001) / (query_weight + 0.0000001);

			//cout << "match_words_count = " << matched_words_count << "\n";
			//cout << "weight = " << match_weight << "\n";
			//cout << "query_weight = " << query_weight << "\n";
		}

		double norm_order_weight = 1.0;
		{
			/* ## from python
				def get_order_weight(self, query_tokens, span_word_matches):
					query_pairs = {}
					total_query_pairs = 1
					for first in xrange(len(query_tokens)):
						for second in xrange(first + 1, len(query_tokens)):
							key = (query_tokens[first], query_tokens[second])
							query_pairs.setdefault(key, 0)
							query_pairs[key] += 1
							total_query_pairs += 1
					matched_pairs = 1
					for first in xrange(len(span_word_matches)):
						for second in xrange(first + 1, len(span_word_matches)):
							key = (span_word_matches[first][1],span_word_matches[second][1])
							if key in query_pairs and query_pairs[key] > 0:
								query_pairs[key] -= 1
								matched_pairs += 1
					norm_order_weight = float(matched_pairs) / total_query_pairs
					return norm_order_weight
			*/
			int totalBigrams = tokensInQuery - 1;
			int bigramsInMatch = 0;
			for (int occurenceIndex = 0; occurenceIndex < (int)match.SegmentOccurences.size() - 1; ++occurenceIndex) {
				int firstKeyId = match.SegmentOccurences[occurenceIndex].second;
				int secondKeyId = match.SegmentOccurences[occurenceIndex + 1].second;
				if (secondKeyId - firstKeyId == 1) {
					++bigramsInMatch;
				}
			}
			norm_order_weight = totalBigrams > 0 ?  1.0 + (double)bigramsInMatch / totalBigrams : 2.0;
		}
		//cout << "norm order weight: " << norm_order_weight << "\n";

		/* ## from python
		span_len = span_word_matches[-1][0][0] - span_word_matches[0][0][0] + 1.0
		weight = norm_match_weight * norm_order_weight / span_len
		weight *= max_case_weight > 0 and 1.1 or 1.0
		matched_words_count = len(set(token for _, token in span_word_matches))
		return (matched_words_count, weight)
		*/
		int span_len = (match.SegmentOccurences.rbegin()->first >> WEIGHT_BITS) -
					   (match.SegmentOccurences.begin()->first >> WEIGHT_BITS) + 1;
		//cout << "span_len: " << span_len << "\n";
		double weight = norm_match_weight * norm_order_weight / span_len;
		weight *= max_case_weight > 0 ? 1.1 : 1.0;
		return pair<int, double>(matched_words_count, weight);
	}

	static inline int GetMinValueIndex(const int* positions, const vector<TOccurences>& occurences) {
		int minValueIndex = -1;
		for (TWordIndex index = 0; index < occurences.size(); ++index) {
			if ((int)occurences[index].size() <= positions[index] + 1) {
				continue;
			}
			if (minValueIndex == -1 || occurences[index][positions[index]] < occurences[minValueIndex][positions[minValueIndex]]) {
				minValueIndex = index;
			}
		}
		return minValueIndex;
	}

	void ConstructMatches(const vector<TOccurences>& occurences,
					      const int* localIndex2KeyIndex,
						  const unsigned long long* keyIndex2freq,
						  const double minMatchWeight2Consider,
						  const unordered_set<TObjectId>* objects2ConsiderPtr,
						  vector<TSegmentMatches>* segmentMatchesPtr) {
		vector<TSegmentMatches>& segmentMatches = *segmentMatchesPtr;
		// only one word
		if (occurences.size() == 1) {
		    long long prevSegmentId = -1;
		    for (int position = 0; position < (int)occurences[0].size(); ++position) {
			const TOccurence occurence = occurences[0][position];
			const TSegmentId segmentId = GetSegmentId(occurence);
			if ((long long)segmentId == prevSegmentId) {
			    continue;
			}
			prevSegmentId = segmentId;
			const TObjectId objectId = GetObjectId(occurence);
			const TPosition positionInSegment = GetSegmentPosition(occurence);
			const TWeight occWeight = GetWeight(occurence);
			if (objects2ConsiderPtr != NULL &&
						objects2ConsiderPtr->find(objectId) == objects2ConsiderPtr->end()) {
				continue;
			}
			const TPositionAndWeight posAndWeight = occWeight + (positionInSegment << WEIGHT_BITS);
			const TPosWeightWithWIndex posWeightWIndex = TPosWeightWithWIndex(posAndWeight, localIndex2KeyIndex[0]);
			segmentMatches.push_back(TSegmentMatches(segmentId, objectId));
			segmentMatches.rbegin()->SegmentOccurences.push_back(posWeightWIndex);
		    }
		    return;
		}
		// more than one word 
		int positions[MAX_WORDS4QUERY]{0};
		int iter = 0;
		while (true) {
			++iter;
			int toIncrease = GetMinValueIndex(&positions[0], occurences);
			if (toIncrease == -1) {
				break;
			}
			const TOccurence occurence = occurences[toIncrease][positions[toIncrease]];
			const TSegmentId segmentId = GetSegmentId(occurence);
			const TObjectId objectId = GetObjectId(occurence);
			const TPosition positionInSegment = GetSegmentPosition(occurence);
			const TWeight occWeight = GetWeight(occurence);
			bool consider = true;
			if (objects2ConsiderPtr != NULL &&
						objects2ConsiderPtr->find(objectId) == objects2ConsiderPtr->end()) {
				consider = false;
			}
			if (consider) {
				const TPositionAndWeight posAndWeight = occWeight + (positionInSegment << WEIGHT_BITS);
				const TPosWeightWithWIndex posWeightWIndex = TPosWeightWithWIndex(posAndWeight, localIndex2KeyIndex[toIncrease]);
				if (segmentMatches.size() == 0 || segmentMatches.rbegin()->Segment != segmentId) {
					if (segmentMatches.size() > 0 &&
								SegmentMatchSimpleWeight(*segmentMatches.rbegin(), keyIndex2freq) < minMatchWeight2Consider) {
						segmentMatches.rbegin()->Segment = segmentId;
						segmentMatches.rbegin()->Object = objectId;
						segmentMatches.rbegin()->SegmentOccurences.resize(1);
						segmentMatches.rbegin()->SegmentOccurences[0] = posWeightWIndex;
					} else {
						segmentMatches.push_back(TSegmentMatches(segmentId, objectId));
						segmentMatches.rbegin()->SegmentOccurences.push_back(posWeightWIndex);
					}
				} else {
					segmentMatches.rbegin()->SegmentOccurences.push_back(posWeightWIndex);
				}
			}
			++positions[toIncrease];
		}
	}

vector<string> SplitString(const string& sourceString, const string& delimiter, bool dropEmptyChunks = true) {
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
		startPos = delimPos + delimiter.size();
	}
	if (startPos != sourceString.size() || !dropEmptyChunks) {
		out.push_back(sourceString.substr(startPos, sourceString.size() - startPos));
	}
	return out;
}

//public:
	string ExecuteQuery(const vector<TKey>& keys,
						const int firstResult2Return,
						const int results2Return,
						const unordered_set<TObjectId>* objects2ConsiderPtr) {
		vector<TOccurences> keysOccurences;
		int localIndex2KeyIndex[MAX_WORDS4QUERY] = { -1 };
		unsigned long long keyIndex2offset[MAX_WORDS4QUERY] = { 0 };
		unsigned long long keyIndex2freq[MAX_WORDS4QUERY] = { 0 };


		const unsigned long long MAX_OCCURENCES_IN_TOTAL = 50000000; //50mln

                //TTime startTime = GetTime();

		vector< pair<unsigned long long, int> > freqAndKeyIndex;
		//upload freqs and offsets
		for (int keyIndex = 0; keyIndex < min(MAX_WORDS4QUERY, (int)keys.size()); ++keyIndex) {
			const TKey& key = keys[keyIndex];
			if ((*Key2FreqAndLocationPtr).find(key) == (*Key2FreqAndLocationPtr).end()) {
			    continue;
			}
			unsigned long long keyFreq = (*Key2FreqAndLocationPtr)[key].first;
			unsigned long long offset = (*Key2FreqAndLocationPtr)[key].second;
			freqAndKeyIndex.push_back(pair<unsigned long long, int>(keyFreq, keyIndex));
			keyIndex2freq[keyIndex] = keyFreq;
			keyIndex2offset[keyIndex] = offset;
		}
		sort(freqAndKeyIndex.begin(), freqAndKeyIndex.end());
		//choose cut
		unsigned long long accumulFreq = 0;
		double maxPossibleMatchWeight = 0.0;
		int keys2consider = 0;
		for (keys2consider = 0; keys2consider < min(MAX_KEYS2CONSIDER, (int)freqAndKeyIndex.size()); ++keys2consider) {
			localIndex2KeyIndex[keys2consider] = freqAndKeyIndex[keys2consider].second;
			accumulFreq += freqAndKeyIndex[keys2consider].first;
			maxPossibleMatchWeight += Freq2IdfFreq(freqAndKeyIndex[keys2consider].first);
			if (accumulFreq > MAX_OCCURENCES_IN_TOTAL) {
				break;
			}
		}
		const double minMatchWeight2Consider = maxPossibleMatchWeight * CRUDE_FILTER_TRIM_PROPORTION;
		//cout << "select keys: " << GetElapsedInSeconds(startTime, GetTime()) << "\n";
		//cout.flush();
		//startTime = GetTime(); 
		//upload occurences
		//TODO: m
		for (int localKeyIndex = 0; localKeyIndex < keys2consider; ++localKeyIndex) {
			int keyIndex = localIndex2KeyIndex[localKeyIndex];
			keysOccurences.push_back(TOccurences(keyIndex2freq[keyIndex], 0));
		    fseek(OccurencesFilePtr, keyIndex2offset[keyIndex], SEEK_SET);
		    fread(&(*keysOccurences.rbegin())[0], sizeof(unsigned long long), keyIndex2freq[keyIndex], OccurencesFilePtr);
		    //cout << "test: " << *(keysOccurences.rbegin()->rbegin()) << " size: " << keysOccurences.rbegin()->size() << "\n";
		}

		//cout << "upload occs : " << GetElapsedInSeconds(startTime, GetTime()) << "\n";
		//startTime = GetTime(); 

		vector<TSegmentMatches> segmentMatches;
		ConstructMatches(keysOccurences,
							&localIndex2KeyIndex[0],
							&keyIndex2freq[0],
							minMatchWeight2Consider,
							objects2ConsiderPtr,
							&segmentMatches);

		//cout << "construct matches : " << GetElapsedInSeconds(startTime, GetTime()) << "\n";
		//startTime = GetTime(); 

		unordered_map<TObjectId, double> objectWeights;
		unordered_map<TObjectId, double> objectMatchesCount;
		unordered_map<TObjectId, double> objectMaxMatchWeight;
		vector< pair<double, TObjectId> > objectsRanking;
		{

			for (int occIndex = 0; occIndex < (int)segmentMatches.size(); ++occIndex) {
				double weight = SegmentMatchSimpleWeight1(segmentMatches[occIndex], &keyIndex2freq[0]);
				TObjectId object = segmentMatches[occIndex].Object;
				if (objectWeights.find(object) == objectWeights.end()) {
					objectWeights[object] = weight;
					objectMatchesCount[object] = 1;
					objectMaxMatchWeight[object] = weight;
				} else {
					objectWeights[object] += weight;
					++objectMatchesCount[object];
					objectMaxMatchWeight[object] = max(objectMaxMatchWeight[object], weight);
				}
			}


			for (auto objectWeightIter = objectWeights.begin();
										 objectWeightIter != objectWeights.end();
										 ++objectWeightIter) {
			        // correct: obj.weight = max. weight * log(matches_count)
				double matches_count = objectMatchesCount[objectWeightIter->first];
				double max_weight = objectMaxMatchWeight[objectWeightIter->first];
				double avg_score = objectWeightIter->second / matches_count;
				double normalized_score = max_weight + avg_score * log(1.0 + matches_count) / 1000000;
				objectsRanking.push_back( pair<double, TObjectId>(normalized_score, objectWeightIter->first) );
			}
			sort(objectsRanking.begin(), objectsRanking.end(), std::greater<pair<double, TObjectId> >());
		}

		//cout << "objects ranking : " << GetElapsedInSeconds(startTime, GetTime()) << "\n";
		//startTime = GetTime(); 


		typedef pair<int, double> TSegmentRelevance;
		typedef pair<TSegmentRelevance, int> TSegmentRelevanceAndIndex;
		typedef priority_queue<pair<double, int>, vector<TSegmentRelevanceAndIndex>, std::greater<TSegmentRelevanceAndIndex> > TPriorityQueue;
		unordered_map<TObjectId, TPriorityQueue> selectedObjectsBestMatches;
		unordered_map<TObjectId, int> selectedObjectsMatchesCount;

		for (int object_index = firstResult2Return;
						object_index < min((int)objectsRanking.size(), firstResult2Return + results2Return);
						++object_index) {
			TObjectId objectId = objectsRanking[object_index].second;
			selectedObjectsBestMatches[objectId] = TPriorityQueue();
			selectedObjectsMatchesCount[objectId] = 0;
		}

		if (selectedObjectsBestMatches.size() > 0) {//select top matches for the selected objects
			for (int occIndex = 0; occIndex < (int)segmentMatches.size(); ++occIndex) {
				TObjectId objectId = segmentMatches[occIndex].Object;
				if (selectedObjectsMatchesCount.find(objectId) == selectedObjectsMatchesCount.end()) {
					continue;
				}
				++selectedObjectsMatchesCount[objectId];
				segmentMatches[occIndex] = SelectShortestSpan(segmentMatches[occIndex]);
				TSegmentMatches& refinedMatch = segmentMatches[occIndex];
				TSegmentRelevance relevance = CalcRelevance(refinedMatch, keys, keyIndex2freq);
				selectedObjectsBestMatches[objectId].push(TSegmentRelevanceAndIndex(relevance, occIndex));
				if (selectedObjectsBestMatches[objectId].size() > MAX_SEGMENTS_PER_OBJECT) {
					selectedObjectsBestMatches[objectId].pop();
				}
			}
		}

		//cout << "top segments of selected objects: " << GetElapsedInSeconds(startTime, GetTime()) << "\n";
		//startTime = GetTime(); 


		//dump
		string matchesDump = std::to_string(objectsRanking.size()) + "<:::>";
		for (int object_index = firstResult2Return;
						object_index < min((int)objectsRanking.size(), firstResult2Return + results2Return);
						++object_index) {
			TObjectId objectId = objectsRanking[object_index].second;
			double objectRelevance = objectsRanking[object_index].first;
			int matchesCount = selectedObjectsMatchesCount[objectId];

			matchesDump += std::to_string(objectId) + ":" + std::to_string(objectRelevance) + ":" + std::to_string(matchesCount) + "||";
			while (selectedObjectsBestMatches[objectId].size() > 0) {
				const TSegmentRelevance& relevance = selectedObjectsBestMatches[objectId].top().first;
				const TSegmentMatches& match = segmentMatches[selectedObjectsBestMatches[objectId].top().second];
				selectedObjectsBestMatches[objectId].pop();
				string matchStr = std::to_string(match.Segment) + ":" +
								  std::to_string(relevance.first) + ":" +
								  std::to_string(relevance.second) + "|";
				for (auto occurenceIt = match.SegmentOccurences.begin();
															occurenceIt != match.SegmentOccurences.end();
															++occurenceIt) {
					matchStr += std::to_string((occurenceIt->first >> WEIGHT_BITS)) + "," +
								std::to_string((occurenceIt->first % (1 << WEIGHT_BITS))) + "," +
								std::to_string(occurenceIt->second) + ";";
				}
				matchesDump += matchStr + "}";
			}
			matchesDump += "<:::>";
		}

		//cout << "dump: " << GetElapsedInSeconds(startTime, GetTime()) << "\n";
		//cout.flush();
		//startTime = GetTime(); 

		return matchesDump;
	}
public:
	char* ExecuteQuery(const char* keysStr,
			    const int firstResult2Return,
		            const int results2Return,
			    const char* objectsStr) {
	       string delimiter = "<>";
	       vector<TKey> keys = SplitString(string(keysStr), delimiter);
	       vector<string> objects2considerAsStrings = SplitString(string(objectsStr), delimiter);
	       unordered_set<TObjectId> objects2consider;
	       for (auto iter = objects2considerAsStrings.begin(); iter != objects2considerAsStrings.end(); ++iter) {
		    objects2consider.insert(strtol(iter->c_str(), NULL, 10));
	       }
	       string output;
	       if (objects2consider.size() > 0) {
		   output = ExecuteQuery(keys, firstResult2Return, results2Return, &objects2consider);
	       } else {
	           output = ExecuteQuery(keys, firstResult2Return, results2Return, NULL);
	       }
	       return strdup(output.c_str());
	}
};




