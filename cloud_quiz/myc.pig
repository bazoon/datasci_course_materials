register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar
-- raw = LOAD 'uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-000' USING TextLoader as (line:chararray);
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/cse344-test-file';

ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);
subjects = group ntriples by (subject);
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count;
group_by_count = group count_by_subject by $1;
count_count = foreach group_by_count generate $0,COUNT(count_by_subject);

store count_by_object_ordered into '/user/hadoop/example-results' using PigStorage();
