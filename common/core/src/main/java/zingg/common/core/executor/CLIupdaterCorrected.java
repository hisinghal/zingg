
public class CliUpdater extends Labeller<Source, Destination, Record, Column, Type> {

	public static final string scannerString = $ {config.scannerString} // [0129]

	int readCliInput() {
		try {
			Scanner sc = new Scanner(System.in);

			while (!sc.hasNext(scannerString)) {
				sc.next();
				System.out.println("Nope, please enter one of the allowed options!");
			}
			String word = sc.next();
			int selection = Integer.parseInt(word);
			return selection;
		} catch (Exception error) {
			Log.error(error.message());
			if (isDebugEnabled) error.printStackTrace();
		} finally {
			sc.close();
		}
	}

	protected int displayRecordsAndGetUserInput(ZFrame<D, R, C> records, String preMessage, String postMessage) {
		labelDataViewHelper.displayRecords(records, preMessage, postMessage);
		int selection = readCliInput();
		return selection;
	}


	public ZFrame<D, R, C> processRecordsCli(ZFrame<D, R, C>  lines) throws ZinggClientException {
		LOG.info("Processing Records for CLI Labelling");
		if (lines != null && lines.count() > 0) {
			trainingDataModel.printMarkedRecordsStat(
			    trainingDataModel.getPositivePairsCount(),
			    trainingDataModel.getNegativePairsCount(),
			    trainingDataModel.getNotSurePairsCount(),
			    trainingDataModel.getTotalCount()
			);

			lines = lines.cache();
			//			List<C> displayCols = getLabelDataViewHelper().getDisplayColumns(lines, args);
			ZidAndFieldDefSelector zidAndFieldDefSelector = new ZidAndFieldDefSelector(args.getFieldDefinition(), false, args.getShowConcise());
			//have to introduce as snowframe can not handle row.getAs with column
			//name and row and lines are out of order for the code to work properly
			//snow getAsString expects row to have same struc as dataframe which is
			//not happening
			ZFrame<D, R, C> clusterIdZFrame = labelDataViewHelper.getClusterIdsFrame(lines);
			List<R>  clusterIDs = labelDataViewHelper.getClusterIds(clusterIdZFrame);
			try {
				ZFrame<D, R, C>  updatedRecords = null;
				updatedRecords = updatedRecords(lines, clusterIDs, clusterIdZFrame);
				LOG.info("Processing finished.");
				return updatedRecords;
			} catch (Exception e) {
				if (LOG.isDebugEnabled()) {
					e.printStackTrace();
				}
				LOG.error("Labelling error has occured " + e.getMessage());
				throw new ZinggClientException("An error has occured while Labelling.", e);
			}
		} else {
			LOG.info("It seems there are no unmarked records at this moment. Please run findTrainingData job to build some pairs to be labelled and then run this labeler.");
			return null;
		}
	}


	public ZFrame<D, R, C> updatedRecords(ZFrame<D, R, C> lines, List<R> clusterIDs, ZFrame<D, R, C> clusterIdZFrame) {
		double score;
		double prediction;
		int selectedOption = LabelOption.INIT;
		String msg1 = null, msg2 = null;
		int totalPairs = clusterIDs.size();

		LOG.info(" total pairs ", totalPairs, class.name);

		for (int index = 0; index < totalPairs; index++) {

			ZFrame<D, R, C>  currentPair = labelDataViewHelper.getCurrentPair(lines, index, clusterIDs, clusterIdZFrame);

			score = labelDataViewHelper.getScore(currentPair);
			prediction = labelDataViewHelper.getPrediction(currentPair);

			msg1 = labelDataViewHelper.getMsg1(index, totalPairs);
			msg2 = labelDataViewHelper.getMsg2(prediction, score);
			//String msgHeader = msg1 + msg2;

			//					selectedOption = displayRecordsAndGetUserInput(getDSUtil().select(currentPair, displayCols), msg1, msg2);
			selectedOption = displayRecordsAndGetUserInput(currentPair.select(zidAndFieldDefSelector.getCols()), msg1, msg2);
			trainingDataModel.updateLabellerStat(selectedOption, LabelOption.INCREMENT);
			labelDataViewHelper.printMarkedRecordsStat(
			    trainingDataModel.getPositivePairsCount(),
			    trainingDataModel.getNegativePairsCount(),
			    trainingDataModel.getNotSurePairsCount(),
			    trainingDataModel.getTotalCount()
			);
			if (selectedOption == LabelOption.QUIT_LABELING) {
				LOG.info("User has quit in the middle. Updating the records.");
				break;
			}
			updatedRecords = trainingDataModel.updateRecords(selectedOption, currentPair, updatedRecords);
		}
		return updatedRecords;
	}
