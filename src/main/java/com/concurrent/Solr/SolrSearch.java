package com.concurrent.Solr;

import org.apache.solr.client.solrj.SolrQuery;

import java.text.DecimalFormat;

import static java.lang.System.out;

public class SolrSearch {

    public static SolrQuery solrQuery;

    public static void main(String[] args) {
        DecimalFormat df2 = new DecimalFormat("#.####");
        solrQuery = new SolrQuery();
        String query;
        String sort_col = null;
        String column_list = null;
        int batch_size = 5000;
        int concurrency = 1;

        if (args.length < 4) {
            System.out.println("Not all required parameters are passed. Minimum required 4 parameters");
            System.exit(0);
        } else if (args.length == 5) {
            batch_size = Integer.parseInt(args[4]);
        } else if (args.length == 6) {
            batch_size = Integer.parseInt(args[4]);
            concurrency = Integer.parseInt(args[5]);
        } else if (args.length == 7) {
            concurrency = Integer.parseInt(args[5]);
            batch_size = Integer.parseInt(args[4]);
            sort_col = args[6];
        } else if (args.length == 8) {
            concurrency = Integer.parseInt(args[5]);
            batch_size = Integer.parseInt(args[4]);
            sort_col = args[6];
            column_list = args[7];
        }

        String node_ip = args[0];
        String collection_name = args[1];
        query = args[2];
        int limit = Integer.parseInt(args[3]);
        String baseUrl = "http://" + node_ip + ":8983/solr/" + collection_name;
        out.println("Connecting to  : " + baseUrl);
        solrQuery.setQuery(query);
        if (sort_col != null) solrQuery.addSort(sort_col, SolrQuery.ORDER.asc);
        if (column_list != null) solrQuery.setFields(column_list);
        out.println("Query is " + solrQuery.toQueryString());
        out.println("Batch Size " + batch_size);
        SolrSearchQuery q = new SolrSearchQuery();
        q.execute(baseUrl, solrQuery, batch_size, limit, concurrency);
    }
}