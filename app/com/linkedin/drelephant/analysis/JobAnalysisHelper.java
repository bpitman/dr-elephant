package com.linkedin.drelephant.analysis;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.ElephantContext;
import com.linkedin.drelephant.util.InfoExtractor;
import com.linkedin.drelephant.util.Utils;
import models.AppHeuristicResult;
import models.AppHeuristicResultDetails;
import models.AppResult;
import org.apache.log4j.Logger;

/**
 * Helper class to get the analysis results.
 */
public class JobAnalysisHelper {

  private static final Logger logger = Logger.getLogger(Utils.class);

  private static final String UNKNOWN_JOB_TYPE = "Unknown";   // The default job type when the data matches nothing.

  /**
   * Returns the analysed AppResult that could be directly serialized into DB.
   *
   * This method uses fetched data as input, runs all the heuristics on them and loads it into the
   * AppResult model.
   *
   * @throws Exception if the analysis process encountered a problem.
   * @return the analysed AppResult
   */

  public static AppResult getAnalysisResult(HadoopApplicationData data, AnalyticJob analyticJob) {
    // Run all heuristics over the fetched data
    List<HeuristicResult> analysisResults = new ArrayList<HeuristicResult>();
    if (data == null || data.isEmpty()) {
      // Example: a MR job has 0 mappers and 0 reducers
      logger.info("No Data Received for analytic job: " + analyticJob.getAppId());
      analysisResults.add(HeuristicResult.NO_DATA);
    } else {
      List<Heuristic> heuristics = ElephantContext.instance().getHeuristicsForApplicationType(analyticJob.getAppType());
      for (Heuristic heuristic : heuristics) {
        HeuristicResult result = heuristic.apply(data);
        if (result != null) {
          analysisResults.add(result);
        }
      }
    }

    JobType jobType = ElephantContext.instance().matchJobType(data);
    String jobTypeName = jobType == null ? UNKNOWN_JOB_TYPE : jobType.getName();

    HadoopMetricsAggregator hadoopMetricsAggregator =
        ElephantContext.instance().getAggregatorForApplicationType(analyticJob.getAppType());
    hadoopMetricsAggregator.aggregate(data);
    HadoopAggregatedData hadoopAggregatedData = hadoopMetricsAggregator.getResult();

    // Load app information
    AppResult result = new AppResult();
    result.id = Utils
        .truncateField(analyticJob.getAppId(), AppResult.ID_LIMIT, analyticJob.getAppId());
    result.trackingUrl = Utils.truncateField(analyticJob.getTrackingUrl(), AppResult.TRACKING_URL_LIMIT, analyticJob.getAppId());
    result.queueName = Utils.truncateField(analyticJob.getQueueName(), AppResult.QUEUE_NAME_LIMIT, analyticJob.getAppId());
    result.username = Utils.truncateField(analyticJob.getUser(), AppResult.USERNAME_LIMIT, analyticJob.getAppId());
    result.startTime = analyticJob.getStartTime();
    result.finishTime = analyticJob.getFinishTime();
    result.name = Utils.truncateField(analyticJob.getName(), AppResult.APP_NAME_LIMIT, analyticJob.getAppId());
    result.jobType = Utils.truncateField(jobTypeName, AppResult.JOBTYPE_LIMIT, analyticJob.getAppId());
    result.resourceUsed = hadoopAggregatedData.getResourceUsed();
    result.totalDelay = hadoopAggregatedData.getTotalDelay();
    result.resourceWasted = hadoopAggregatedData.getResourceWasted();

    // Load App Heuristic information
    int jobScore = 0;
    result.yarnAppHeuristicResults = new ArrayList<AppHeuristicResult>();
    Severity worstSeverity = Severity.NONE;
    for (HeuristicResult heuristicResult : analysisResults) {
      AppHeuristicResult detail = new AppHeuristicResult();
      detail.heuristicClass = Utils.truncateField(heuristicResult.getHeuristicClassName(),
          AppHeuristicResult.HEURISTIC_CLASS_LIMIT, analyticJob.getAppId());
      detail.heuristicName = Utils.truncateField(heuristicResult.getHeuristicName(),
          AppHeuristicResult.HEURISTIC_NAME_LIMIT, analyticJob.getAppId());
      detail.severity = heuristicResult.getSeverity();
      detail.score = heuristicResult.getScore();

      // Load Heuristic Details
      for (HeuristicResultDetails heuristicResultDetails : heuristicResult.getHeuristicResultDetails()) {
        AppHeuristicResultDetails heuristicDetail = new AppHeuristicResultDetails();
        heuristicDetail.yarnAppHeuristicResult = detail;
        heuristicDetail.name = Utils.truncateField(heuristicResultDetails.getName(),
            AppHeuristicResultDetails.NAME_LIMIT, analyticJob.getAppId());
        heuristicDetail.value = Utils.truncateField(heuristicResultDetails.getValue(),
            AppHeuristicResultDetails.VALUE_LIMIT, analyticJob.getAppId());
        heuristicDetail.details = Utils.truncateField(heuristicResultDetails.getDetails(),
            AppHeuristicResultDetails.DETAILS_LIMIT, analyticJob.getAppId());
        // This was added for AnalyticTest. Commenting this out to fix a bug. Also disabling AnalyticJobTest.
        //detail.yarnAppHeuristicResultDetails = new ArrayList<AppHeuristicResultDetails>();
        detail.yarnAppHeuristicResultDetails.add(heuristicDetail);
      }
      result.yarnAppHeuristicResults.add(detail);
      worstSeverity = Severity.max(worstSeverity, detail.severity);
      jobScore += detail.score;
    }
    result.severity = worstSeverity;
    result.score = jobScore;

    // Retrieve information from job configuration like scheduler information and store them into result.
    InfoExtractor.loadInfo(result, data);

    return result;
  }

}
