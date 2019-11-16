# ☁️ AWS-EMR-Collocation-Extraction ☁️

## Abstract
This app automatically extract collocations from the Google 2-grams dataset using Amazon Elastic Map Reduce.
- The data: http://www.commondatastorage.googleapis.com/books/syntactic-ngrams/index.html

## Collocations
A collocation is a sequence of words or terms that co-occur more often than would be expected by chance. The identification of collocations - such as 'crystal clear', 'cosmetic surgery'.
We used Normalized Pointwise Mutual Information (NPMI), in order to decide whether a given pair of ordered words is a collocation, where two ordered words with a high NPMI value are expected to be a collocation.

## Normalized PMI
PMI is a measure of association used in information theory and statistics.
Given two words w1 and w2 and a corpus, we define normalized pmi as follows:

## 
The purpose of this application is to learn and experiment with a MapReduce method, which required 3 different
stages of MapReduces to get the desired result for the entirely corpus (Google 2-grams dataset).
In addition to experience with EMR - Elastic Map Reduce, which is a service offered by Amazon.


## How to run the project –
1. Create in S3 the path – hadoop-map-reduce-collocation-extraction/HadoopMapReduceJars/ and upload the tree jar file to it.
2. Create in S3 the path – 
  • hadoop-map-reduce-collocation-extraction/OutputStepOne
  • hadoop-map-reduce-collocation-extraction/OutputStepTwo
  • hadoop-map-reduce-collocation-extraction/OutputStepThree
  
### Run – 
  ➢ Javac LocalApp.java 
  ➢ Java LocalApp minPmi relMinPmi
  
### The output will be on –
  • hadoop-map-reduce-collocation-extraction/OutputStepThree/subdir
  
### Job flow –
1. Step one –
  At this stage of the work, the CE_Mapper extracts the required information from each data line and creates 4 different keys and sends   them to CE_Reducer with a value equal to occurrences which extruct from the line.
    - CE_Mapper - creates 4 different keys as follow –
      a. Type DECADE - { * , *, decade} 
      b. Type FIRST - { w1 , *, decade} 
      c. Type SECOND - { * , w2, decade} 
      d. Type NGRAM - { w1 , w2, decade}
      
    - CE_Reducer – 
      - At this stage of the work, the CE_Reducer gets the keys in order so that when a
        pair of words w1, w2 comes to CE_Reducer, we move the pair to the next level when the corresponding value contains the                   information for c (w1), N and c (w1, w2).  
        
        o The KV will be: { key = {w1, w2, decade}, value={ c (w1), c (w1, w2), 0, N}
        
      - Additionally, when a SECOND key arrives, it moves to the next step with a value where the information for c (w2)
      
        o The KV will be: { key = { * , w2, decade}, value={ 0, 0, c(w2) , 0}
        
      The keys will appear in the following order – (for x>y, w1>z1) :
      { * , *, x} > { w1 , *, x} > { w1 , any word(w1), x} > { z1 , *, x} > {z1 , any word(z1), x} > … > { * , any word(w1), x} >
      { * , any word(w1), x}> { * , any word(…), x} >{ * , *, y}
      
2. Step two – At this point we read the results from step one when each key is either NGRAM or SECOND. The NGRAM key contains all the information needed to perform the calculation, except c(w2) .This information is stored in SECOND keys corresponding to the same w2, so these keys will arrive earlier so that when a NGRAM key arrives we will have the total information for c(w2) (And we can perform the PMI calculation).
The keys appear in the following order – (for x>y, w1>z1) { * , w2, x} > {any word(w2) , w2, x} > { * , z2, x} > {any word(z2) , z2, x} > >… > { * , c2, x} > {any word(c2) , c2, x} > …
• CE_Mapper – only passing the key to CE_Reducer.
• CE_Reducer – - When a NGRAM key is obtained, the information for c (w2) corresponding to that decade already exists in memory, so the calculation can be performed for npmi. At this stage we create 2 different KV and move them to the next level.
o Type PMI: : { key = { * , * , decade} , value = { pmi(w1, w2)} o Type NGRAM: { key = {w1, w2, decade}, value = pmi(w1, w2)}
3. Step three – At this stage we read the information and know what the PMI calculation is for each pair of words. Now remains the sum of the general pmi for each decade, and then calculate the relative pmi.
• CE_Mapper – only passing the key to CE_Reducer.
• CE_Reducer – If a PMI key is received, the CE_Reducer only summarizes all the value and saves them in memory. Immediately after that, all NGRAM keys will arrive.
If a NGRAM key is received, the CE_Reducer performs the relative pmi calculation and checks whether to save the above key with the relevant results in the results file. (if npmi(w1, w2) > minPmi or rMinPmi(w1, w2) > rMinPmi). In this case the result will be o { key = { w1 , w2 , decade} , value = { npmi(w1, w2), rMinPmi(w1, w2)}
The keys appear in the following order – (for x>y) { * , *, x} > {any word(w1) , {any word(w2), x} > { * , *, y} > { any word(w1) , {any word(w2),
y}
