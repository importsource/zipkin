/**
 * Copyright 2015-2017 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.elasticsearch.http;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import okio.Buffer;
import zipkin.Span;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.Callback;

import static zipkin.internal.ApplyTimestampAndDuration.guessTimestamp;
import static zipkin.internal.Util.propagateIfFatal;
import static zipkin.storage.elasticsearch.http.ElasticsearchHttpSpanStore.SERVICE_SPAN;

final class ElasticsearchHttpSpanConsumer implements AsyncSpanConsumer {

  final ElasticsearchHttpStorage es;
  final IndexNameFormatter indexNameFormatter;

  ElasticsearchHttpSpanConsumer(ElasticsearchHttpStorage es) {
    this.es = es;
    this.indexNameFormatter = es.indexNameFormatter();
  }

  @Override public void accept(List<Span> spans, Callback<Void> callback) {
    if (spans.isEmpty()) {
      callback.onSuccess(null);
      return;
    }
    try {
      HttpBulkSpanIndexer spanIndexer = new HttpBulkSpanIndexer(es);
      Map<String, Set<ServiceSpan>> indexToServiceSpans = indexSpans(spanIndexer, spans);
      if (indexToServiceSpans.isEmpty()) {
        spanIndexer.execute(callback);
        return;
      }
      HttpBulkIndexer<ServiceSpan> serviceSpanIndexer =
          new HttpBulkIndexer<ServiceSpan>(SERVICE_SPAN, es) {
            Buffer buffer = new Buffer();

            @Override byte[] toJsonBytes(ServiceSpan serviceSpan) {
              try {
                adapter.toJson(buffer, serviceSpan);
                return buffer.readByteArray();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
      for (Map.Entry<String, Set<ServiceSpan>> entry : indexToServiceSpans.entrySet()) {
        String index = entry.getKey();
        for (ServiceSpan serviceSpan : entry.getValue()) {
          serviceSpanIndexer.add(index, serviceSpan,
              serviceSpan.serviceName + "|" + serviceSpan.spanName);
        }
      }
      spanIndexer.execute(new Callback<Void>() {
        @Override public void onSuccess(Void value) {
          serviceSpanIndexer.execute(callback);
        }

        @Override public void onError(Throwable t) {
          callback.onError(t);
        }
      });
    } catch (Throwable t) {
      propagateIfFatal(t);
      callback.onError(t);
    }
  }

  static final JsonAdapter<ServiceSpan> adapter =
      new Moshi.Builder().build().adapter(ServiceSpan.class);

  static final class ServiceSpan {

    final String serviceName;
    final String spanName;

    ServiceSpan(String serviceName, String spanName) {
      this.serviceName = serviceName;
      this.spanName = spanName;
    }

    @Override public boolean equals(Object o) {
      ServiceSpan that = (ServiceSpan) o;
      return (this.serviceName.equals(that.serviceName))
          && (this.spanName.equals(that.spanName));
    }

    @Override public int hashCode() {
      int h = 1;
      h *= 1000003;
      h ^= serviceName.hashCode();
      h *= 1000003;
      h ^= spanName.hashCode();
      return h;
    }
  }

  Map<String, Set<ServiceSpan>> indexSpans(HttpBulkSpanIndexer indexer, List<Span> spans)
      throws IOException {
    Map<String, Set<ServiceSpan>> indexToServiceSpans = new LinkedHashMap<>();
    for (Span span : spans) {
      Long timestamp = guessTimestamp(span);
      Long timestampMillis;
      String index; // which index to store this span into
      if (timestamp != null) {
        timestampMillis = TimeUnit.MICROSECONDS.toMillis(timestamp);
        index = indexNameFormatter.indexNameForTimestamp(timestampMillis);
        for (String serviceName : span.serviceNames()) {
          if (!span.name.isEmpty()) {
            Set<ServiceSpan> serviceSpans = indexToServiceSpans.get(index);
            if (serviceSpans == null) {
              indexToServiceSpans.put(index, serviceSpans = new LinkedHashSet<>());
            }
            serviceSpans.add(new ServiceSpan(serviceName, span.name));
          }
        }
      } else {
        timestampMillis = null;
        index = indexNameFormatter.indexNameForTimestamp(System.currentTimeMillis());
      }
      indexer.add(index, span, timestampMillis);
    }
    return indexToServiceSpans;
  }
}
