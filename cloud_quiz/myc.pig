register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar;
raw = LOAD 'uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);
-- raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file' USING TextLoader as (line:chararray);
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);


subjects = group ntriples by (subject);
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count PARALLEL 50;
group_by_count = group count_by_subject by $1;
count_count = foreach group_by_count generate $0,COUNT(count_by_subject) PARALLEL 50;

store count_count into '/user/hadoop/example-results' using PigStorage();
