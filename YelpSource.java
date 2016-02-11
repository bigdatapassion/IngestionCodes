import com.hexacorp.sas.flume.yelpOld.TwoStepOAuth;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.scribe.builder.ServiceBuilder;
import org.scribe.model.Token;
import org.scribe.oauth.OAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YelpSource  extends AbstractSource implements EventDrivenSource, Configurable {

//    private OAuthService service;
//    private Token accessToken;
    private YelpAPI yelpAPI;
    private String keywords;
    private String business;
    private String city;
    private Logger LOG = LoggerFactory.getLogger(YelpSource.class);
    final ChannelProcessor channelProcessor = getChannelProcessor();

    public void configure(Context context) {
        String CONSUMER_KEY= context.getString("CONSUMER_KEY");
        String CONSUMER_SECRET = context.getString("CONSUMER_SECRET");
        String TOKEN = context.getString("TOKEN");
        String TOKEN_SECRET = context.getString("TOKEN_SECRET");
        business = context.getString("business");
        city = context.getString("city");
        keywords = context.getString("keywords");

        yelpAPI = new YelpAPI(CONSUMER_KEY, CONSUMER_SECRET,TOKEN,TOKEN_SECRET);
    }

    @Override
    public void start(){
        yelpAPI.queryAPI(business, city, 10);
        super.start();
    }

    @Override
    public void stop() {
        super.stop();

    }


}
