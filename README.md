# ☁️ AWS-EMR-Collocation-Extraction ☁️

## Abstract
This app automatically extract collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.
- The data: ![project structure](http://www.commondatastorage.googleapis.com/books/syntactic-ngrams/index.html)

## Collocations
A collocation is a sequence of words or terms that co-occur more often than would be expected by chance. The identification of collocations - such as 'crystal clear', 'cosmetic surgery'.
We used Normalized Pointwise Mutual Information (NPMI), in order to decide whether a given pair of ordered words is a collocation, where two ordered words with a high NPMI value are expected to be a collocation.

## Normalized PMI
PMI is a measure of association used in information theory and statistics.
Given two words w1 and w2 and a corpus, we define normalized pmi as follows:

## 
The purpose of this application is to learn and experiment with a map reduce calculation method, which required 3 different
stages of map reduction to get the desired result for the entirely corpus (Google 2-grams dataset).
In addition to experience with EMR - Elastic Map Reduce, which is a service offered by Amazon.
