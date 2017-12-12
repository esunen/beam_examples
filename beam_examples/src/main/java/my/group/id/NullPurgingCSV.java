package my.group.id;

//import my.group.id.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
//import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
//import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
//import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
//import org.apache.beam.sdk.transforms.SimpleFunction;
//import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
//import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
//import org.apache.beam.sdk.values.KV;


public class NullPurgingCSV {

	public static void main(String[] args) {

	    String[] source_file_names = {
	  		"PEOPLE.csv",
	    		"DOCUMENT_DISCIPLINES.csv",
	    		"DOCUMENT_ADDRESSES.csv",
	    		"DOCUMENT_ADDRESS_DATA.csv",
	    		"DOCUMENT_DATA.csv",
	    		"DOCUMENT_INDICATORS.csv",
	    		"DOCUMENT_SIGNATURES.csv",
	    		"DOCUMENT_SIGNATURE_DATA.csv",
	    		"LOCATIONS.csv"
	    	};
	    String[] csv_headers = {
	    		// PEOPLE.csv
	    		"\"ID\",\"FIRST_NAME\",\"SURNAME\",\"DESCRIPTION\",\"GENDER\",\"BIRTHDATE\",\"NATIONALITY_ID\",\"TYPE\",\"RESEARCHERID\",\"CREATED_AT\",\"UPDATED_AT\",\"NUM_AUTHORSHIPS\",\"FULL_NAME\",\"ACTIVE\",\"MAIN_ORGANIZATION_IDS\",\"MAIN_DISCIPLINE_IDS\",\"MAIN_ORGANIZATIONS_UP_TO_DATE\",\"MAIN_DISCIPLINES_UP_TO_DATE\",\"ORCID_ID\",\"FIRST_NAME_NORMAL_FORM\",\"SURNAME_NORMAL_FORM\",\"NORMALIZATION_STATUS\"",
	    		
	    		// DOCUMENT_DISCIPLINES.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"DISCIPLINE_ID\",\"SOURCE_ID\",\"DOCUMENT_DATA_ID\"",

	    		// DOCUMENT_ADDRESSES.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"ADDRESS_ID\"",
	    		
	    		// DOCUMENT_ADDRESS_DATA.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"DOCUMENT_ADDRESS_ID\",\"ORDINAL\",\"TEXT\",\"ADDRESS_ID\",\"IS_CORRESPONDENCE_ADDRESS\",\"SOURCE_ID\",\"DOCUMENT_DATA_ID\"",
	    		
	    		// DOCUMENT_DATA.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"DOI\",\"ISI_ID\",\"PMID\",\"ARTN\",\"TITLE\",\"START_PAGE\",\"END_PAGE\",\"CITABLE\",\"CITABLE_TR\",\"PUBLICATION_DATE\",\"YEAR\",\"MONTH\",\"DAY\",\"VOLUME\",\"ISSUE\",\"PUBLICATION_TITLE\",\"ISSN\",\"ESSN\",\"JRNL20\",\"ISO_ABBREVIATURE\",\"IMPACT_FACTOR\",\"QUARTILE\",\"LANGUAGE\",\"NLM_ID\",\"PUBLICATION_TYPE_ID\"",
	    		
	    		// DOCUMENT_INDICATORS.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"INDICATORS_COMPUTATION_ID\",\"SOURCE_ID\",\"NUM_CITATIONS\",\"CITATIONS_MRFY\",\"JXCR\",\"JRCI\",\"CROWN_INDEX\",\"CITATION_PERCENTILE\",\"DISCIPLINE_ID\",\"JIF\",\"JIF_QUARTILE\",\"RNIF\"",
	    		
	    		// DOCUMENT_SIGNATURES.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"SIGNATURE_ID\",\"AUTHOR_ID\",\"AUTHOR_TYPE\",\"AUTHOR_CLASS_ID\"",
	    		
	    		// DOCUMENT_SIGNATURE_DATA.csv
	    		"\"ID\",\"DOCUMENT_ID\",\"DOCUMENT_SIGNATURE_ID\",\"ORDINAL\",\"AUTHOR_TYPE\",\"TEXT\",\"SIGNATURE_ID\",\"EMAIL\",\"SOURCE_ID\",\"IS_SHORT_SIGNATURE\",\"LONG_DOC_SIGNATURE_DATA_ID\",\"DOCUMENT_DATA_ID\",\"IS_CORRESPONDING_AUTHOR\",\"ORCID_ID\",\"RESEARCHERID\"",
	    		
	    		// LOCATIONS.csv
	    		"\"ID\",\"NAME\",\"LANGUAGE\",\"DESCRIPTION\",\"TYPE\",\"LOCATION_LEVEL\",\"GEONAMEID\",\"ISO2_CODE\",\"ISO3_CODE\",\"ISO_NUMERIC\",\"POSTAL_CODE\",\"FIPS_CODE\",\"POSTAL_CODE_REGEXP\",\"LATITUDE\",\"LONGITUDE\",\"POPULATION\",\"CONTINENT_ID\",\"COUNTRY_ID\",\"ADMIN1_ID\",\"ADMIN2_ID\",\"ADMIN3_ID\",\"ADMIN4_ID\",\"CREATED_AT\",\"UPDATED_AT\",\"PARENT_ID\",\"FULL_NAME\",\"NUTS_CODE\",\"NORMALIZED_NAME\",\"CITY_ID\""		
	    	};
	    
	    Pipeline[] p = {null, null, null, null, null, null, null, null, null};
	    PipelineResult[] r = {null, null, null, null, null, null, null, null, null};

	    for(int i = 0; i < source_file_names.length;  i++) {
		    // Create the Pipeline object with default arguments.
		    p[i] = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		    p[i].apply(TextIO.read().from("gs://bb2-row-data/export_bb2.csv/" + source_file_names[i]))

		     .apply("ExtractNullWords", ParDo.of(new DoFn<String, String>() {
	            @ProcessElement
	            public void processElement(ProcessContext c) {
	            	String line = c.element();
	            	if(!line.startsWith("\"")) {
		            	line = line.replaceAll(",\\(null\\)", ",");
		            	line = line.replaceAll("^\\(null\\),", ",");
		            	c.output(line);
	            	}
	            }
	          }))
		    
		     .apply(TextIO.write().withoutSharding().withHeader(csv_headers[i])
		    		 	.to("gs://bb2-row-data/filtered_output/" + source_file_names[i]));
	    }   
	   
	    // Run the pipelines.	    
	    for(int i = 0; i < source_file_names.length;  i++) {
	    	r[i] = p[i].run();
	    }
	    
	    // Wait the pipelines until finish
	    for(int i = 0; i < source_file_names.length;  i++) {
	    	r[i].waitUntilFinish();
	    }
	}
}
