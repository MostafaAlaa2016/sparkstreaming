#### Shell script to monitor spark streaming job if it failed it will start again else do nothing checking ###
#### @2018-03-27 Moustafa Alaa mohamedamostafa@etisalat.ae ####





current_date_run=`date --date="yesterday" +%Y%m%d`
a=`yarn application --list -appTypes SPARK -appStates RUNNING | grep 'STREAMING::: SIM_SWAP_FRAU' | awk -F" " '$5!=0{print $1}' | wc -l`
echo "Checking Spark Job status at $current_date_run"
echo "Job Status is $a"
if [  "$a" -eq "0" ]; 
then  
echo "Job starting now as new instance $current_date_run." 
nohup spark-submit --master yarn --deploy-mode client --class com.etisalat.rtm.Application --conf spark.driver.extraJavaOptions='-Dlog4j.configuration=file:/rtmstaging/etisalat/MostafaAlaa/log4j.properties' --conf spark.storage.memoryFraction=0.6 --conf spark.shuffle.memoryFraction=0.2 --conf spark.yarn.executor.memoryOverhead=2048 --executor-memory 2g --num-executors 12 --executor-cores 2 --driver-memory 10g /rtmstaging/etisalat/MostafaAlaa/SPARK-1.0-jar-with-dependencies.jar /BINS/RAW_ZONE/RAW_FILES/CBCMSOH_FILTER 3 'adcb_integration' /BINS/RAW_ZONE/RAW_FILES/CBCMSOH_FILTER/checkpoint  /rtmstaging/BINS/RAW_ZONE/RAW_FILES/CBCMSOH_FILTER/Processed/processed_files.txt /rtmstaging/BINS/RAW_ZONE/RAW_FILES_ARCHIVE/CBCMSOH_FILTER/ &

echo "Job started $current_date_run." 
fi
