#!/bin/bash
###############################################################################################################################################################
##																																							  #
##																																							  #
## Master_Copied() function will read the file processedfile.txt. and update the ToBeCopyFileList.txt.														  #
## It validate for the CopiedFileList whether its already copied  if not will place the row into ToBeCopyFileList.txt file.									  #
## get_Copied_Files() function will copy the files present in BeCopyFileList.txt list file and update the CopiedFileList list file.							  #
## truncate_main_file() once the files are archived to the target directory then processedfile.txt list file will be truncated.								  #
## get_SparkFunction() will submit the spark code.																											  #
## get_Concatenate() as per the requirement concatenate all the files  into a single file with date extension and moved the file to the target directory.	  #
## At the end with if else condition default all the master functions will run without passing any parameter.												  #
## if we pass the parameter Copied than only Master_Copied() function will be executed or if parameter Spark then only get_SparkFunction() will be executed.  #
## target where the files will archived.																													  #
## Sparkpath path of the processfile where files paths  are present.																						  #
## ToBeCopyFileList which consider only those lines from processfile.txt file which need to copy.															  #
## CopiedFileList once copied will be updated about the path of those files.																				  #
## movesource path for the another fucntion where no. of files reside.																						  #
## movetarget path where movesource path files are moved once copied into a single file.																	  # 
###############################################################################################################################################################

target="/bigpfstest/DPI_INVESTIG/Sadiq/TARGET"
SparkPath="/bigpfstest/DPI_INVESTIG/Sadiq/processedfile.txt"
ToBeCopyFileList="/bigpfstest/DPI_INVESTIG/Sadiq/COPIED/ToBeCopyFileList.txt"
CopiedFileList="/bigpfstest/DPI_INVESTIG/Sadiq/COPIED/CopiedFileList.txt"
DATE=`date +"%Y%m%d%H%M%S"`
movesource="/bigpfstest/DPI_INVESTIG/Sadiq/TEST"
movetarget="/bigpfstest/DPI_INVESTIG/Sadiq/TEST_ARCH"

Master_Copied()
{
get_ToBeCopyFiles()
{
        truncate --size 0 $ToBeCopyFileList

        # get to be pulled fixed files
        while read -r line
        do
                file=$line

                if ! grep -Fxq $file $CopiedFileList
                then
                        echo $file >> $ToBeCopyFileList
                else
                        echo "stream" $file " already Copied"
                fi
        done < $SparkPath
}
get_Copied_Files()
{
        while read -r line;
        do
                file=$line
                #cp -v $file $target 2 > /dev/null
                cp -v $file $target 2>/dev/null
                echo $file >> $CopiedFileList
        done < $ToBeCopyFileList
}

truncate_main_file()
{
if ! grep -Fxq $SparkPath $CopiedFileList
then
truncate --size 0 $SparkPath
fi
}
get_ToBeCopyFiles
get_Copied_Files
truncate_main_file
}

get_SparkFunction()
{
echo "Spark command"
}

get_Concatenate()
{
cat $movesource/* >> $movesource/CBCM_$DATE.out 2>/dev/null
mv -v $movesource/*.bin $movetarget 2>/dev/null
}

if [[ $# == 0 ]]
then
Master_Copied
get_SparkFunction
get_Concatenate
elif [[ -n $1 ]]
then
option=$1
fi
case $option in copied)
      Master_Copied
        ;;
                spark)
        get_SparkFunction
        ;;
esac
