/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.PrestoClientException;
import io.airlift.http.client.HttpStatus;

import java.io.EOFException;
import java.net.ConnectException;
import java.util.Arrays;

import static com.google.common.base.Throwables.getCausalChain;
import static java.lang.String.format;

public class ErrorMessages
{
    private static final String PRESTO_COORDINATOR_NOT_FOUND = "There was a problem with a response from Presto Coordinator.\n";
    private static final String PRESTO_COORDINATOR_404 = "Presto HTTP interface returned 404 (file not found).\n";

    private static final String TECHNICAL_DETAILS_HEADER = "\n=========   TECHNICAL DETAILS   =========\n";
    private static final String SESSION_INTRO = "[ Session information ]\n";
    private static final String ERROR_MESSAGE_INTRO = "[ Error message ]\n";
    private static final String STACKTRACE_INTRO = "[ Stack trace ]\n";
    private static final String RESPONSE_INTRO = "[ Plain text HTTP response ]\n";
    private static final String TECHNICAL_DETAILS_END = "========= TECHNICAL DETAILS END =========\n\n";

    private enum Tip {
        VERIFY_PRESTO_RUNNING("Verify that Presto is running on %s."),
        DEFINE_SERVER_AS_CLI_PARAM("Use '--server' argument when starting Presto CLI to define server host and port."),
        CHECK_NETWORK("Check the network conditions between client and server."),
        USE_DEBUG_MODE("Use '--debug' argument to get more technical details."),
        CHECK_OTHER_HTTP_SERVICE("Make sure that none other HTTP service is running on %s."),
        CLIENT_IS_UP_TO_DATE_WITH_SERVER("Update CLI to match Presto server version.");

        private static final String TIPS_INTRO = "To solve this problem you may try to:\n";

        private final String message;

        private Tip(String message)
        {
            this.message = message;
        }

        @Override
        public String toString()
        {
            return message;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        private static class Builder
        {
            private StringBuilder builder = new StringBuilder();

            public Builder()
            {
                builder.append(TIPS_INTRO);
            }

            public Builder addTip(Tip tip, Object ...toFormat)
            {
                builder.append(format(" * " + tip.toString() + "\n", toFormat));
                return this;
            }

            public String build()
            {
                return builder.toString();
            }
        }
    }

    private ErrorMessages()
    {}

    public static String createErrorMessage(Throwable throwable, ClientSession session)
    {
        if (throwable instanceof PrestoClientException) {
            return prestoClientExceptionErrorMesage((PrestoClientException) throwable, session);
        }
        else {
            return runtimeExceptionErrorMessage(throwable, session);
        }
    }

    private static String prestoClientExceptionErrorMesage(PrestoClientException clientException, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();
        boolean wasErrorIdentified = false;

        if (clientException.getResponse().getStatusCode() == HttpStatus.NOT_FOUND.code()) {
            serverFileNotFoundErrorMessage(builder, session);
            wasErrorIdentified = true;
        }

        if (!wasErrorIdentified) {
            builder.append(clientException.getMessage());
        }

        if (session.isDebug()) {
            technicalDetailsPrestoServerExceptionErrorMessage(builder, clientException, session);
        }

        return builder.toString();
    }

    private static String runtimeExceptionErrorMessage(Throwable throwable, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();

        if (getCausalChain(throwable).stream().anyMatch(x -> x instanceof EOFException || x instanceof ConnectException)) {
            serverNotFoundErrorMessage(builder, session);
        }
        else {
            // We have no clue about what went wrong, just display what we obtained.
            builder.append("Error running command:\n" + throwable.getMessage() + "\n");
        }

        if (session.isDebug()) {
            technicalDetailsRuntimeExceptionErrorMessage(builder, throwable, session);
        }

        return builder.toString();
    }

    private static String getStackTraceString(Throwable throwable)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(Arrays.stream(throwable.getStackTrace()).map(x -> x.toString()).reduce("", (x, y) -> x + y + "\n"));
        if (throwable.getCause() != null) {
            builder.append("caused by:\n");
            builder.append(getStackTraceString(throwable.getCause()));
        }
        return builder.toString();
    }

    //region Messages for given problems
    private static void serverNotFoundErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_COORDINATOR_NOT_FOUND);
        Tip.Builder tipsBuilder = Tip.builder();
        tipsBuilder.addTip(Tip.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tip.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tip.CHECK_NETWORK).build();
        tipsBuilder.addTip(Tip.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tip.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tip.CHECK_NETWORK).build();
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tip.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void serverFileNotFoundErrorMessage(StringBuilder builder, ClientSession session)
    {
        builder.append(PRESTO_COORDINATOR_404);
        Tip.Builder tipsBuilder = Tip.builder();
        tipsBuilder.addTip(Tip.VERIFY_PRESTO_RUNNING, session.getServer())
                .addTip(Tip.DEFINE_SERVER_AS_CLI_PARAM)
                .addTip(Tip.CHECK_NETWORK)
                .addTip(Tip.CHECK_OTHER_HTTP_SERVICE, session.getServer())
                .addTip(Tip.CLIENT_IS_UP_TO_DATE_WITH_SERVER);
        if (!session.isDebug()) {
            tipsBuilder.addTip(Tip.USE_DEBUG_MODE);
        }
        builder.append(tipsBuilder.build());
    }

    private static void technicalDetailsPrestoServerExceptionErrorMessage(StringBuilder builder, PrestoClientException serverException, ClientSession session)
    {
        builder.append(TECHNICAL_DETAILS_HEADER);
        builder.append(SESSION_INTRO);
        builder.append(session + "\n\n");
        builder.append(STACKTRACE_INTRO);
        if (serverException.getServerException().isPresent()) {
            builder.append(serverException.getServerException().get().getStackTraceString() + "\n\n");
        }
        else {
            builder.append(getStackTraceString(serverException) + "\n");
        }

        builder.append(RESPONSE_INTRO);
        builder.append(serverException.getResponse().getHeaders().toString() + "\n");
        builder.append(serverException.getResponse().getResponseBody() + "\n");

        builder.append(TECHNICAL_DETAILS_END);
    }

    private static void technicalDetailsRuntimeExceptionErrorMessage(StringBuilder builder, Throwable throwable, ClientSession session)
    {
        builder.append(TECHNICAL_DETAILS_HEADER);
        builder.append(ERROR_MESSAGE_INTRO);
        builder.append(throwable.getMessage() + "\n\n");
        builder.append(SESSION_INTRO);
        builder.append(session + "\n\n");
        builder.append(STACKTRACE_INTRO);
        builder.append(getStackTraceString(throwable));
        builder.append(TECHNICAL_DETAILS_END);
    }
    //endregion
}
