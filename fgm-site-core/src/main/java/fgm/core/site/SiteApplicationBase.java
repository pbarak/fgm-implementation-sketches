package fgm.core.site;

import java.util.concurrent.CountDownLatch;

public class SiteApplicationBase {

    static final CountDownLatch latch = new CountDownLatch(1);
    static final String SITE_PROCESSOR_NAME = "SiteProcessorNode";
    static final String SOURCE_NODE_NAME = "SourceNode";

    static final String SHUT_DOWN_HOOK = "streams-shutdown-hook";

    // JSON key for Serde config
    static final String JSON_SERDES_CLASS_NAME = "JsonPOJOClass";

    protected SiteApplicationBase() {
    }


}

