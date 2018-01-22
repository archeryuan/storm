package com.sa.storm.sns.util;

import java.util.ArrayList;
import java.util.Date;

import com.sa.graph.ch.object.BackendJob;
import com.sa.graph.ch.object.Person;
import com.sa.graph.ch.service.FBUserDataService;

public class TestGraphDBUtil {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("need args ");
			return;
		}
		String email = args[0];
		String pId = args[1];

		FBUserDataService service = new FBUserDataService();

		BackendJob job = new BackendJob();
		job.setJobId(136L);
		job.setJobName("New World Email List 1");
		job.setCreated(new Date());


		Person p = new Person();
		p.setPersonId(Long.parseLong(pId));
		p.setAddr("aa");
		p.setName("carrot");
		p.setCity("HongKong");
		p.setGender("male");
		p.setPEmail1(email);
		try {
			service.saveFBUser(p, job);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
