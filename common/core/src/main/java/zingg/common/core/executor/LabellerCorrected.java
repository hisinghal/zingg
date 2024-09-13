package zingg.common.core.executor;

import java.util.List;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import zingg.common.client.ILabelDataViewHelper;
import zingg.common.client.ITrainingDataModel;
import zingg.common.client.ZFrame;
import zingg.common.client.ZinggClientException;
import zingg.common.client.cols.ZidAndFieldDefSelector;
import zingg.common.client.options.ZinggOptions;
import zingg.common.client.util.ColName;

public abstract class Labeller<Source, Destination, Record, Column, Type> extends ZinggBase<Source, Destination, Record, Column, Type> {

	
	   public enum LabelOption {
        QUIT_LABELING(9),
        INCREMENT(1),
        INIT(-1);

        private final int value;

        LabelOption(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
	
	private static final long serialVersionUID = 1L;
	protected static String name = "zingg.common.core.executor.Labeller";
	public static final Log LOG = LogFactory.getLog(Labeller.class);
	protected ITrainingDataModel<S, D, R, C> trainingDataModel;
	protected ILabelDataViewHelper<S, D, R, C> labelDataViewHelper;

	public Labeller() {
		this.trainingDataModel = new TrainingDataModel<S, D, R, C, T>(getContext(), getClientOptions());
		this.labelDataViewHelper = new LabelDataViewHelper<S, D, R, C, T>(getContext(), getClientOptions());
		setZinggOption(ZinggOptions.LABEL);
	}

	public void execute() throws ZinggClientException {
		try {
			LOG.info("Reading inputs for labelling phase ...");
			trainingDataModel.setMarkedRecordsStat(getMarkedRecords());
			ZFrame<D, R, C>  unmarkedRecords = getUnmarkedRecords();
			ZFrame<D, R, C>  updatedLabelledRecords = processRecordsCli(unmarkedRecords);
			trainingDataModel.writeLabelledOutput(updatedLabelledRecords, args);
			LOG.info("Finished labelling phase");
		} catch (Exception e) {
			e.printStackTrace();
			throw new ZinggClientException(e.getMessage());
		}
	}
}

