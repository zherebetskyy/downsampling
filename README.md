# downsampling
This scala script contains DownSampling class that 
1) Does ID downsampling 
- reads data in a specified time-range for specified src-partiotion from an indicated location, 
- selects distinct IDs, 
- downsamples a fraction of IDs,
- writes these IDs with the additional columns src and dt to HDFS 
2) Does records downsampling using the IDs
- reads IDs from the specified location fro specified ID-downsampling date,
- reads all daily data for a specified date,
- selects all the records from the daily data that are associated with IDs,
