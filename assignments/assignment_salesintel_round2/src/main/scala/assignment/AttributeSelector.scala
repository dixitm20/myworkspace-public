package assignment

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, max, row_number}

object AttributeSelector {
  def main(args: Array[String]) {

    // >>> BEGIN SECTION >>>: Parameter Parsing
    // INFO: Assigning defaults to parameter variables. Assumption are that these are not the valid values
    //       for corresponding parameters & will never be passed as arguments to to respective parameters.
    //       If after parsing parameters these still have same default values then this implies that the value of
    //       that parameter has not been provided to the application & we abort the app run.
    var param_inputPath = ""
    var param_outputPath = ""
    var param_batch_id = 0
    var param_lastFullBatchId = 0
    var param_createFullBatch = true
    // String container for the param_createFullBatch to check if this is passed or not
    var param_container_createFullBatch = ""

    // Function to check if given paramName has been passed to the application
    def param_check(paramName: String, emptyVal: String, actualVal: String): Unit = {
      if (emptyVal != actualVal) println(s"\tSetting $paramName=$actualVal") else
        throw new Exception(s"Missing required argument: $paramName value")
    }

    if (args.length != 5) {
      println("""Usage: AttributeSelector "{inputPath}" "{outputPath}" {batch_id} {lastFullBatchId} {createFullBatch}""")
      throw new Exception("Invalid arguments list. Please refer the usage for details")
    } else {
      param_inputPath = args(0)
      param_outputPath = args(1)
      param_batch_id = args(2).toInt
      param_lastFullBatchId = if (args(3).toUpperCase == "NONE") -1 else args(3).toInt
      param_container_createFullBatch = args(4).toLowerCase

      println("Following are the details of input parameters passed to the application:")
      param_check("inputPath", "", param_inputPath)
      param_check("outputPath", "", param_outputPath)
      param_check("batch_id", "0", param_batch_id.toString)
      param_check("lastFullBatchId", "0", param_lastFullBatchId.toString)
      param_check("createFullBatch", "", param_container_createFullBatch)

      param_createFullBatch = param_container_createFullBatch.toBoolean

      if (param_batch_id <= param_lastFullBatchId)
        throw new Exception("Invalid Argument Value For batch_id. batch_id should always be greater than lastFullBatchId")
    }
    // <<< END SECTION <<<: Parameter Parsing


    // >>> BEGIN SECTION >>>: Set parameters
    //-------------------------------------------------------------------------------------
    val inputPath: String = param_inputPath
    val outputPath: String = param_outputPath
    val batch_id: Int = param_batch_id
    // ASSUMPTION: We will never have batch_id which are less than 0
    // INFO: If we do not pass lastFullBatchId then we assign default of -1
    val lastFullBatchId: Int = param_lastFullBatchId
    val createFullBatch: Boolean = param_createFullBatch
    //-------------------------------------------------------------------------------------
    // <<< END SECTION <<<: Set parameters


    val spark = SparkSession.builder()
      .appName("Attribute Selector")
      .config("spark.master", "local[8]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    // spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


    // >>> BEGIN SECTION >>>: Algorithm description
    //-------------------------------------------------------------------------------------
    /*

    ATTRIBUTES RULE FOR REF:
    - For each company, take the set of all attributes with highest probability. There will be
          only one attribute value for each attribute_type.
    - If probabilities are the same, then take the attribute value from the newer batch,
         followed by the lower source id as a tie-breaker

    For application of the above 2 rules I have used the single window functions. The window function partitions the
    input dataset on the basis of company_id & attribute_type  and then sorts the data on attribute_probability(DESC),
    batch_id(DESC) & source_id(ASC) and assigns a rank as per this partitioning and ordering.
    The sql equivalent of this step can be written as:
    ROW_NUMBER() OVER (PARTITION BY COMPANY_ID, ATTRIBUTE_TYPE ORDER BY ATTRIBUTE_PROB DESC NULLS LAST,
                       BATCH_ID DESC NULLS LAST, SOURCE_ID ASC NULLS LAST).
    Once we have this rank available with us we simply select the record with rank 1.

    By utilizing this method of attribute rule application I am able to handle the scenarios of incremental
    feed generation and full feed generation through the same process. As the only difference between these
    two feeds generation is in the input that is given to this process (we include the last full batch_id
    data from target into algorithm processing input dataframe for incremental feed & for full feed the
    input includes the all existing batch_id from target starting from last full extract till the
    most recent batch_id) and the application of filter to the output of the attribute application process (
    for full feed generation no filter is needed & for incremental feed generation output of the above process is
    filtered with a condition of selecting only those batch_id which are new in the current run)

  */
    //-------------------------------------------------------------------------------------
    // <<< END SECTION <<<: Algorithm description


    // >>> BEGIN SECTION >>>: Read source
    //-------------------------------------------------------------------------------------
    // INFO: If this is not the first full load then pull latest incremental
    //       batch id from target since last full batch load else set it to -1
    val latestIncmntlBatchIdSinceLastFullBatch = if (lastFullBatchId == -1) -1 else {
      spark.read.parquet(s"$outputPath")
        .where(s"batch_id >= $lastFullBatchId")
        .select(max("batch_id"))
        .collect()(0).getInt(0)
    }

    println(s"Latest incremental batch id since last " +
      s"full batch load: $latestIncmntlBatchIdSinceLastFullBatch")

    // INFO: Using the condition:
    //       batch_id > $latestIncmntlBatchIdSinceLastFullBatch AND batch_id <= $batch_id
    //       to pull from source. By using the latestIncmntlBatchIdSinceLastFullBatch in
    //       the source pull we are able to automatically fill any gaps that may get added
    //       if the run for any intermediary batches is delayed or skipped.
    println(s"Pulling batch_id from source using the filter: " +
      s"batch_id > $latestIncmntlBatchIdSinceLastFullBatch AND batch_id <= $batch_id")

    println(s"\n==> 1. DATA FROM SOURCE TO BE USED FOR CURRENT BATCH PROCESSING: " +
      s"batch_id > $latestIncmntlBatchIdSinceLastFullBatch AND batch_id <= $batch_id\n")

    val srcdf = spark.read.parquet(s"$inputPath")
      .where(s"batch_id > $latestIncmntlBatchIdSinceLastFullBatch " +
        s"AND batch_id <= $batch_id")
    //-------------------------------------------------------------------------------------
    // <<< END SECTION <<<: Read source


    // >>> BEGIN SECTION >>>: Append target data
    //-------------------------------------------------------------------------------------
    // INFO: If this is the first run then we will use this value for processing and
    //       if not then we append the data from target accordingly
    val procdf = if (lastFullBatchId == -1) {
      println("As this is the first run of the application no data from target " +
        "needs to be appended to source")
      println("\n==> 2. DATA FROM TARGET TO BE MERGED WITH SOURCE FOR THE FIRST RUN FOR " +
        "EITHER FULL LOAD/INCREMENTAL BATCH: NULL\n")

      srcdf
    } else {
      if (createFullBatch) {
        // INFO: For creation of full batch we pull from target all the
        //       partitions data since the last full load
        println(s"As the createFullBatch: true we pull & append to source data " +
          s"all the partitions data since the last full load from target. we pull " +
          s"data from target using the condition: batch_id >= $lastFullBatchId")
        println(s"\n==> 2. DATA FROM TARGET TO BE MERGED WITH SOURCE FOR " +
          s"FULL LOAD BATCH: batch_id >= $lastFullBatchId\n")

        val tgtdf = spark.read.parquet(s"$outputPath")
          .where(s"batch_id >= $lastFullBatchId")

        srcdf.union(tgtdf)
      } else {
        // INFO: For creation of incremental batch we pull from target only the
        //       partition which has full load data
        println(s"As the createFullBatch: false we pull & append to source data " +
          s"only the partitions data of last full load from target. we pull " +
          s"data from target using the condition: batch_id = $lastFullBatchId")
        println(s"\n==> 2. DATA FROM TARGET TO BE MERGED WITH SOURCE FOR " +
          s"INCREMENTAL BATCH: batch_id = $lastFullBatchId\n")

        val tgtdf = spark.read.parquet(s"$outputPath")
          .where(s"batch_id = $lastFullBatchId")

        srcdf.union(tgtdf)
      }
    }
    //-------------------------------------------------------------------------------------
    // <<< END SECTION <<<: Append target data


    // >>> BEGIN SECTION >>>: Apply attribute rules
    //-------------------------------------------------------------------------------------
    // Apply the following attribute rules on the source data
    // Attribute Rules
    // - For each company, take the set of all attributes with highest probability.
    //       There will be only one attribute value for each attribute_type.
    // - If probabilities are the same, then take the attribute value from the newer batch,
    //      followed by the lower source id as a tie-breaker.

    println("==> 3. DATA FROM POINTER 1 & 2 WILL PROCESSED THOROUGH THE COMMON " +
      "ATTRIBUTE SELECTION PROCESSING ENGINE\n")

    println("NOTE: The attribute selection processing engine will fetch max attribute_probablity " +
      "\n\tper company, attribute_type within the complete input (source <<POINT 1>> + target <<POINT 2>>) and " +
      "\n\tthen select the only those attributes which match max attribute_probablity and belong " +
      "\n\tto the latest batch, followed by the lower source id as a tie-breaker\n")

    // INFO: Below window will be used to get Max attribute_prob for each company_id and atrribute_type.
    //       This window forms the base for ranking to choose the Max attribute_prob record. In case of
    //       any conflicts use the logic of selecting  the attribute value from the newer batch,
    //       followed by the lower source id as a tie-breaker
    val windowSpecMaxAttribProbWithTieBreaker = Window
      .partitionBy("company_id", "attribute_type")
      .orderBy(col("attribute_prob").desc_nulls_last,
        col("batch_id").desc_nulls_last, col("source_id").asc_nulls_last)

    // Assign ranking for max attribute_prob usng the tie-breaker window spec of above step
    val rankMaxAttribProbWithTieBreaker = row_number().over(windowSpecMaxAttribProbWithTieBreaker)

    // INFO: Select only those attributes which have maximum attribute probablity
    //       (rank_max_attribute_prob_with_tiebreaker=1) and in case of conflicts choose
    //       record with maximum batch_id, followed by the lower source id as a tie-breaker
    val appliedRuledf = procdf
      .select(
        expr("*"),
        rankMaxAttribProbWithTieBreaker.alias("rank_max_attribute_prob_with_tiebreaker"))
      .where("rank_max_attribute_prob_with_tiebreaker = 1")
      .drop("rank_max_attribute_prob_with_tiebreaker")
    //-------------------------------------------------------------------------------------
    // <<< END SECTION <<<: Apply attribute rules


    // INFO: If createFullBatch is true then we set appliedRuledf as the finaldf.
    //           By including the data from target with the condition: batch_id >= $lastFullBatchId in procdf in section
    //           ">>> BEGIN SECTION >>>: Append target data" we have already included all data of last full batch and remaining
    //           incremental batches. Now once we have this entire data in scope the application of above rules give us
    //           the complete full load data. The only thing that needs to be done is to drop the actual batch_id column
    //           from source and assigning the single batch_id passed to the script for the current run as the batch_id
    //           in target.
    //
    //       If createFullBatch is false then we filter the dataframe appliedRuledf
    //           with condition: batch_id > $latestIncmntlBatchIdSinceLastFullBatch (all new batch_id) and set this
    //           as the finaldf. By including the data from target with the condition: batch_id = $lastFullBatchId
    //           in procdf in section ">>> BEGIN SECTION >>>: Append target data" we have already included all data of
    //           last full batch. Now once we have this data in scope the application of above rules will give us
    //           the all the attributes of last full load with their final values after merge with current batch_id.
    //           Given we are interested only in the affects (changes) due of the new batch we apply the filter:
    //           batch_id > $latestIncmntlBatchIdSinceLastFullBatch to the appliedRuledf and this forms the
    //           final incremental dataframe.The only thing that needs to be done is to drop the actual batch_id column
    //           from source and assigning the single batch_id passed to the script for the current run as the batch_id
    //           in target.
    val finaldf = if (createFullBatch) {
      println("As this is the complete load no filter to the output is requited ")
      println(s"\n==> 4. PROCESSED OUT DATA BY ATTRIBUTE ENGINE NEEDS NO FILTER FOR " +
        s"GENERATING FULL LOAD BATCH INTO BATCH_ID: $batch_id")

      appliedRuledf.drop("batch_id")

    } else {
      println("As this is the incremental load we will filter the output dataset " +
        "to include only the data that is ")
      println(s"\n==> 4. PROCESSED OUT DATA BY ATTRIBUTE ENGINE NEEDS TO BE FILTERED " +
        s"\n\tUSING CONDITION: batch_id > $latestIncmntlBatchIdSinceLastFullBatch FOR " +
        s"GENERATING INCREMENTAL LOAD BATCH INTO BATCH_ID: $batch_id")

      appliedRuledf.where(s"batch_id > $latestIncmntlBatchIdSinceLastFullBatch")
        .drop("batch_id")

    }

    // Cache this dataframe as we run another action on the this dataframe for getting counts
    finaldf.cache()

    finaldf.coalesce(1).write
      .mode(SaveMode.Overwrite)
      .parquet(s"$outputPath/batch_id=$batch_id")

    val outputRecCount = finaldf.count()

    if (createFullBatch) {
      println(s"\n==> 5. FULL BATCH WRITE LOADED RECORD COUNT: " +
        s"$outputRecCount INTO TARGET PARTITION: $outputPath/batch_id=$batch_id")
    } else {
      println(s"\n==> 5. INCREMENTAL BATCH WRITE LOADED RECORD COUNT: " +
        s"$outputRecCount INTO TARGET PARTITION: $outputPath/batch_id=$batch_id")
    }

  }
}