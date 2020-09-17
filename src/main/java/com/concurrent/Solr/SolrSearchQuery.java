package com.concurrent.Solr;

import org.apache.solr.client.solrj.SolrQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SolrSearchQuery {

    public void execute(String baseUrl, SolrQuery query, int batch_size, int limit, int concurrency) {

        final ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        final ExecutorCompletionService<Long> completionService = new ExecutorCompletionService<>(pool);

        int new_limit = limit / concurrency;
        System.out.println("new limit per thread" + new_limit);
        ArrayList<Object> items = getItemList(baseUrl, query, batch_size, concurrency, new_limit);

        List<Future<Long>> futures = new ArrayList<>();
        int i = 0;
        System.out.println("****** Submitting Queries ******");
        while (i < concurrency) {
            SelectQueryCallable callable = new SelectQueryCallable(items);
            futures.add(completionService.submit(callable));
            ++i;
        }


        long resultCount = 0;
        for (Future<Long> future : futures) {
            try {
                Future<Long> completedFuture = completionService.take();
                resultCount = resultCount + completedFuture.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        pool.shutdown();
        pool.shutdownNow();
        System.out.println("****** Total Records fetched are: " + resultCount);
    }

    public ArrayList<Object> getItemList(String baseUrl, SolrQuery query, int batch_size, int concurrency, int new_limit) {
        ArrayList<Object> arlist = new ArrayList<Object>();
        arlist.add(concurrency);

        for (int a = 0; a < concurrency; a++) {
            arlist.add(a * new_limit);
        }
        arlist.add(baseUrl);
        arlist.add(query);
        arlist.add(new_limit);
        arlist.add(batch_size);
        return arlist;
    }

}