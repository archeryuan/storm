package com.sa.storm.sns.bolt.relation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.FacebookHDFSUtil;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import com.sa.ch.aws.s3.FileUploader;
import com.sa.competitorhunter.dao.EstimationRepository;
import com.sa.competitorhunter.dao.RelationHistoryRepository;
import com.sa.competitorhunter.domain.Estimation;
import com.sa.competitorhunter.domain.RelationFansInfo;
import com.sa.competitorhunter.domain.RelationHistory;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskResult;

public class FacebookRelationPersistBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;
	private RelationHistoryRepository relationRepo;
	private EstimationRepository estimationRepo;
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

	private static final Logger log = LoggerFactory.getLogger(FacebookRelationPersistBolt.class);

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		try {
			relationRepo = getSpringContext().getBean(RelationHistoryRepository.class);
			estimationRepo = getSpringContext().getBean(EstimationRepository.class);
		} catch (Exception e) {
			log.error("Error in EsoonMysqlBolt prepare", e);
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void process(Tuple input) throws Exception {
		// TODO Auto-generated method stub
		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		Document doc = docResult.getDocument();
		String hId = doc.getHistoryId();
		String uId = doc.getUserId();
		String pageNames = doc.getUserName();
		String HDFSFile = doc.getFileName();
		String dateStr = DATE_FORMAT.format(new Date());
		Set<RelationFansInfo> relationFansInfo = FacebookHDFSUtil.readFans(HDFSFile, Long.parseLong(hId));
		String fileLocation = FileUploader.uploadFileFromHdfs(dateStr + "-Fans-of-" + pageNames + ".csv", HDFSFile, false);
		try {
			log.info("HDFSFile " + HDFSFile);
			RelationHistory f = relationRepo.findById(Long.parseLong(hId));
			f.setFileLocation(fileLocation);
			f.setStatus(1);
			// persist fans info

			FacebookHDFSUtil.deleteHDFSFile(HDFSFile);
			f.setFansInfos(relationFansInfo);
			relationRepo.saveAndFlush(f);

			// update the estimation

			List<Estimation> estmation = estimationRepo.getByTypeAndUserId((short) 1, Long.parseLong(uId), 2);

			Estimation est = estmation.get(0);
			est.setActualCompletedTime(new Date());
			estimationRepo.saveAndFlush(est);

		} catch (Exception e) {
			log.error("Error in SQLInsert", e);
		}

		TaskResult taskResult = new TaskResult(docResult, TupleDefinition.Result.SUCCESS.getCode());
		emit(input, taskResult, "update mysql status");
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		// TODO Auto-generated method stub
		DocumentResult docResult = (DocumentResult) getInputByClass(input, DocumentResult.class);
		TaskResult error = new TaskResult(docResult, "Error in store record in Mysql ", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
