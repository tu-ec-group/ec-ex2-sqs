package com.aws.sqs;

import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 *
 * Modified version of code example in Chapter 4 of "Java Message Service", 2nd
 * Ed, O'Reilly which replaces JMS with SQS.
 *
 * @author marco peise
 *
 */
public class SqsLender {

	private String requestQ = null;
	private String responseQ = null;
	private AmazonSQS sqs = null;

	public SqsLender(String requestQueue, String responseQueue) {
		this.requestQ = requestQueue;
		this.responseQ = responseQueue;

		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (/path-to-you-user-directory/.aws/credentials).
		 */
		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider("default")
					.getCredentials();
		} catch (Exception e) {
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location (/path-to-you-user-directory/.aws/credentials), and is in valid format.",
					e);
		}

		sqs = new AmazonSQSClient(credentials);
		Region awsRegion = Region.getRegion(Regions.EU_CENTRAL_1);
		sqs.setRegion(awsRegion);
	}

	public void processLoans() {
		// TODO Prepare receive loan request message request.
		//ReceiveMessageRequest receiveLoanRequestMessageRequest = ...
		while (true) {
			// Prepare loan response message request.
			SendMessageRequest loanResponseMessageRequest = new SendMessageRequest();
			loanResponseMessageRequest.setQueueUrl(responseQ);
			// TODO Check request queue for loan requests.
			// List<Message> messages = ...
			//for (Message loanRequestMessage : messages) {
			//	StringTokenizer st = new StringTokenizer(
			//			loanRequestMessage.getBody(), ",");
			//	double salary = Double.valueOf(st.nextToken().trim())
			//			.doubleValue();
			//	double loanAmt = Double.valueOf(st.nextToken().trim())
			//			.doubleValue();
			//	// Determine whether to accept or decline the loan request
			//	boolean accepted = false;
			//	if (loanAmt < 200000) {
			//		accepted = (salary / loanAmt) > .25;
			//	} else {
			//		accepted = (salary / loanAmt) > .33;
			//	}
			//	System.out.println("" + "Percent = " + (salary / loanAmt)
			//			+ ", loan is " + (accepted ? "Accepted!" : "Declined"));
			//	loanResponseMessageRequest
			//			.setMessageBody((accepted ? "Accepted your request for $"
			//					+ loanAmt
			//					: "Declined your request for $" + loanAmt));
			//
			//	for (Entry<String, MessageAttributeValue> entry : loanRequestMessage
			//			.getMessageAttributes().entrySet()) {
			//		if (entry.getKey().equals("uuid")) {
			//			loanResponseMessageRequest.addMessageAttributesEntry(
			//					"uuid", entry.getValue());
			//		}
			//	}
				// TODO Delete loan request message from queue
			// ...
				// TODO Send out the response
			// ...
			//}

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	public static void main(String argv[]) {
        String requestq = "https://sqs.eu-central-1.amazonaws.com/186706155491/LoanRequestQ";
		String responseq = "https://sqs.eu-central-1.amazonaws.com/186706155491/LoanResponseQ";

		SqsLender lender = new SqsLender(requestq, responseq);

		System.out.println("QLender application started");
		System.out.println("Waiting for loan requests...");
		lender.processLoans();
	}
}
