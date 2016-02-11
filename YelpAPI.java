import com.hexacorp.sas.flume.yelpOld.TwoStepOAuth;
import org.apache.flume.Event;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.mortbay.util.ajax.JSON;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;


public class YelpAPI {
    private static final String API_HOST = "api.yelp.com";
    private static final String DEFAULT_TERM = "dinner";
    private static final String DEFAULT_LOCATION = "San Francisco, CA";
    private static final int SEARCH_LIMIT = 20;
    private static final String SEARCH_PATH = "/v2/search";
    private static final String BUSINESS_PATH = "/v2/business";

    private static final String CONSUMER_KEY ="";
    private static final String CONSUMER_SECRET = "";
    private static final String TOKEN ="";
    private static final String TOKEN_SECRET = "";

    private OAuthService service;
    private Token accessToken;

    private Logger LOG = LoggerFactory.getLogger(YelpAPI.class);



    public YelpAPI(String consumerKey, String consumerSecret, String token, String tokenSecret) {
        this.service = new ServiceBuilder().provider(TwoStepOAuth.class).apiKey(consumerKey)
                .apiSecret(consumerSecret).build();
        this.accessToken = new Token(token, tokenSecret);
    }


    private String searchForBusinessByLocation(String term, String location, int offset){
        OAuthRequest request = new OAuthRequest(Verb.GET, "https://" + API_HOST + SEARCH_PATH);
        request.addQuerystringParameter("term", term);
        request.addQuerystringParameter("location", location);
        request.addQuerystringParameter("offset", String.valueOf(offset));
        request.addQuerystringParameter("limit",String.valueOf(20));
        return sendRequestAndGetResponse(request);

    }

    private String sendRequestAndGetResponse(OAuthRequest request) {
        LOG.info("Querying " + request.getCompleteUrl() + " ...");
        this.service.signRequest(this.accessToken, request);
        Response response = request.send();
        return response.getBody();
    }

    public void queryAPI(String term, String location, int limit){
        final HashMap headers = new HashMap();
        headers.put("location", location);
        headers.put("searchDate", new Date());
        for(int i= 0; i < limit; i++) {
            String searchResponseJSON = this.searchForBusinessByLocation(term, location, i * 20);
            JSONParser parser = new JSONParser();
            JSONObject response = null;
            try {
                response = (JSONObject) parser.parse(searchResponseJSON);
            } catch (ParseException pe) {
                LOG.error("Error: could not parse JSON response:");
                LOG.error(searchResponseJSON);
                System.exit(1);
            }
            JSONArray businesses = (JSONArray) response.get("businesses");
            if (businesses.size() == 0) {
                LOG.info("No more business results");
                break;
            }
            for (int j = 0; j < businesses.size(); j++) {
                LOG.info(businesses.get(j).toString());
            }
        }
    }

    public void queryAPI(String term, String location, int limit, ChannelProcessor channelProcessor){
        final HashMap headers = new HashMap();
        headers.put("location", location);
        headers.put("searchDate", new Date());
        for(int i= 0; i < limit; i++){
            String searchResponseJSON =this.searchForBusinessByLocation(term, location, i*20);
            JSONParser parser = new JSONParser();
            JSONObject response = null;
            try {
                response = (JSONObject) parser.parse(searchResponseJSON);
            } catch (ParseException pe) {
                System.out.println("Error: could not parse JSON response:");
                System.out.println(searchResponseJSON);
                System.exit(1);
            }
            JSONArray  businesses = (JSONArray) response.get("businesses");
            if(businesses.size() == 0){
                LOG.info("No more business results");
                System.exit(0);
            }
            for(int j =0 ; j < businesses.size(); j++){
                Event event = EventBuilder.withBody(businesses.get(j).toString().getBytes(),headers);
                channelProcessor.processEvent(event); //new line
            }
        }
    }

    public static void main(String[] args) {
        YelpAPI yelpAPI = new YelpAPI(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);
        yelpAPI.queryAPI("tacobell", "New York", 10);
    }

}

