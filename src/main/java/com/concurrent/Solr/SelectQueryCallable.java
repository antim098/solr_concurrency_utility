package com.concurrent.Solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import java.util.ArrayList;
import java.util.concurrent.Callable;

public class SelectQueryCallable implements Callable<Long> {
    private ArrayList<Object> items;

    SelectQueryCallable(ArrayList<Object> items) {
        this.items = items;
    }

    static String extractInt(String str) {
        // Replacing every non-digit number
        // with a space(" ")
        str = str.replaceAll("[^\\d]", " ");

        // Remove extra spaces from the beginning
        // and the ending of the string
        str = str.trim();

        // Replace all the consecutive white
        // spaces with a single space
        str = str.replaceAll(" +", " ");

        if (str.equals(""))
            return "-1";

        return str;
    }

    @Override
    public Long call() throws Exception {
        long record_count = 0;
        long QTime = 0;
        long num_Found = 0;
        boolean done = false;
        QueryResponse rsp = null;
        SolrDocumentList list = null;
        SolrQuery solrQuery = new SolrQuery();

        int thread_count = (int) items.get(0);
        String baseUrl = (String) items.get(thread_count + 1);
        SolrClient client = new HttpSolrClient.Builder(baseUrl).build();
        solrQuery = ((SolrQuery) items.get(thread_count + 2)).getCopy();
        int limit = (int) items.get(thread_count + 3);
        int batch_size = (int) items.get(thread_count + 4);

        try {
            int thread_no = Integer.parseInt(extractInt(Thread.currentThread().getName()).split(" ")[1]);
            int start = (int) items.get(thread_no);
            System.out.println(Thread.currentThread().getName() + " - thread started executing - start = " + start);
            solrQuery.setRows(batch_size);
            int left_over = limit % batch_size;
            while (record_count < limit && !done) {
                if (limit - record_count == left_over) solrQuery.setRows(left_over);
                //solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
                //out.println("------------before response---------- " + solrQuery.toQueryString());
                solrQuery.setStart(start);
                rsp = client.query(solrQuery, SolrRequest.METHOD.POST);
                //out.println(rsp);
                list = rsp.getResults();
                record_count = record_count + list.size();
                long time = rsp.getQTime();
                QTime = QTime + time;
                start = start + batch_size;
                if (list.isEmpty()) done = true;

            }

            System.out.println(Thread.currentThread().getName() + " - thread finished query in time " + QTime + " Records Read: " + record_count);
            System.out.println("NumFound : " + rsp.getResults().getNumFound());
            list.clear();
            client.close();

        } catch (
                Exception e) {
            e.printStackTrace();
        }
        return record_count;
    }

}