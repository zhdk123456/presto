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
package com.facebook.presto.client;

import com.facebook.presto.spi.PrestoException.PrestoExceptionSerialized;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;

import java.util.Optional;

import static java.lang.String.format;

public class PrestoClientException
        extends RuntimeException
{
    private final String task;
    private final Request request;
    private final JsonResponse<QueryResults> response;
    private final Optional<PrestoExceptionSerialized> serverException;

    public PrestoClientException(String task, Request request, JsonResponse<QueryResults> response)
    {
        this(task, request, response, Optional.empty());
    }

    public PrestoClientException(String task, Request request, JsonResponse<QueryResults> response, PrestoExceptionSerialized serverException)
    {
        this(task, request, response, Optional.of(serverException));
    }

    public PrestoClientException(String task, Request request, JsonResponse<QueryResults> response, Optional<PrestoExceptionSerialized> serverException)
    {
        super(getDefaultMessage(task, request, response, serverException));
        this.task = task;
        this.request = request;
        this.response = response;
        this.serverException = serverException;
    }

    private static String getDefaultMessage(String task, Request request, JsonResponse<QueryResults> response, Optional<PrestoExceptionSerialized> serverException)
    {
        StringBuilder message = new StringBuilder();
        message.append(format("Error %s at %s returned HTTP response code %s.\n", task, request.getUri(), response.getStatusCode()));
        if (serverException.isPresent()) {
            message.append(format("PrestoException %s with code %s.\n", serverException.get().getErrorCode().getName(), serverException.get().getErrorCode().getCode()));
            message.append(format("PrestoException message: \n%s\n", serverException.get().getMessage()));
        }
        else {
            message.append(format("Response info:\n%s\n", response));
            message.append(format("Response body:\n%s\n", response.getResponseBody()));
            if (response.hasValue()) {
                message.append(format("Response status:\n%s\n", response.getStatusMessage()));
            }
        }
        return message.toString();
    }

    public String getTask()
    {
        return task;
    }

    public Request getRequest()
    {
        return request;
    }

    public JsonResponse<QueryResults> getResponse()
    {
        return response;
    }

    public Optional<PrestoExceptionSerialized> getServerException()
    {
        return serverException;
    }
}
