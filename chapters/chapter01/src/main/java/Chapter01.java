import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

import java.util.*;

/**
 * 文章投票的功能
 * Redis中key形式为(XXX:id)
 */
public class Chapter01 {
    private static final int ONE_WEEK_IN_SECONDS = 7 * 86400;
    private static final int VOTE_SCORE = 432;
    private static final int ARTICLES_PER_PAGE = 25;

    public static void main(String[] args) {
        new Chapter01().run();
    }

    public void run() {
        //Jedis为连接开发工具，jedis对象线程不安全，多线程下使用同一个Jedis对象会出现并发问题。
        // 为了避免每次使用Jedis对象时都需要重新创建，Jedis提供了JedisPool。JedisPool是线程安全的连接池
        Jedis conn = new Jedis("localhost");
        conn.select(15);

        String articleId = postArticle(
                conn, "username", "A title", "http://www.google.com");
        System.out.println("We posted a new article with id: " + articleId);
        System.out.println("Its HASH looks like:");
        Map<String,String> articleData = conn.hgetAll("article:" + articleId);
        for (Map.Entry<String,String> entry : articleData.entrySet()){
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }

        System.out.println();

        articleVote(conn, "other_user", "article:" + articleId);
        String votes = conn.hget("article:" + articleId, "votes");
        System.out.println("We voted for the article, it now has votes: " + votes);
        assert Integer.parseInt(votes) > 1;

        System.out.println("The currently highest-scoring articles are:");
        List<Map<String,String>> articles = getArticles(conn, 1);
        printArticles(articles);
        assert articles.size() >= 1;

        addGroups(conn, articleId, new String[]{"new-group"});
        System.out.println("We added the article to a new group, other articles include:");
        articles = getGroupArticles(conn, "new-group", 1);
        printArticles(articles);
        assert articles.size() >= 1;
    }

    /**
     * 发布文章
     * @param conn 连接开发对象
     * @param user 用户
     * @param title 文章标题
     * @param link 文章连接
     * @return
     */
    public String postArticle(Jedis conn, String user, String title, String link) {
        //key为"article:"的value自增
        String articleId = String.valueOf(conn.incr("article:"));

        //key为"voted:文章id"的投票集合，其元素包括用户，过期时间
        String voted = "voted:" + articleId;
        conn.sadd(voted, user);
        conn.expire(voted, ONE_WEEK_IN_SECONDS);

        //key为"article:文章id"的文章散列，包括标题键值对，连接键值对，用户键值对，发布时间键值对，投票数键值对
        long now = System.currentTimeMillis() / 1000;
        String article = "article:" + articleId;
        HashMap<String,String> articleData = new HashMap<String,String>();
        articleData.put("title", title);
        articleData.put("link", link);
        articleData.put("user", user);
        articleData.put("now", String.valueOf(now));
        articleData.put("votes", "1");
        //为散列添加多个键值对
        conn.hmset(article, articleData);

        //key为"score:"的得分有序集合，包括得分键值对，"article:文章id"为成员，分值为投票得分
        conn.zadd("score:", now + VOTE_SCORE, article);

        //key为"score:"的时间有序集合，包括时间键值对，"article:文章id"为成员，分值为发布时间
        conn.zadd("time:", now, article);

        return articleId;
    }

    /**
     * 文章投票
     * @param conn
     * @param user 用户
     * @param article 文章:id
     */
    public void articleVote(Jedis conn, String user, String article) {
        //截止时间
        long cutoff = (System.currentTimeMillis() / 1000) - ONE_WEEK_IN_SECONDS;

        //zscore:通过成员获取有序集合对应的分值
        //是否还能投票(文章发布期满一周之后，将不能再进行投票 ，节约内存)
        if (conn.zscore("time:", article) < cutoff){
            return;
        }

        //得到文章ID
        String articleId = article.substring(article.indexOf(':') + 1);
        //判断该用户是否对文章已投票，通过集合的数据添加判断，1.成功，0.失败
        if (conn.sadd("voted:" + articleId, user) == 1) {
            //在以"score:"为key的得分有序集合中，zincrby：将指定的成员的分值加上increment
            conn.zincrby("score:", VOTE_SCORE, article);
            //在以"article"为key的文章散列中，hincrBy：将指定的键的对应的value加上increment
            conn.hincrBy(article, "votes", 1);
        }
    }


    /**
     *
     * @param conn
     * @param page
     * @return
     */
    public List<Map<String,String>> getArticles(Jedis conn, int page) {
        return getArticles(conn, page, "score:");
    }


    public List<Map<String,String>> getArticles(Jedis conn, int page, String order) {
        int start = (page - 1) * ARTICLES_PER_PAGE;
        int end = start + ARTICLES_PER_PAGE - 1;

        //得分有序集合，zrevrange:给定排名范围内的成员(article:文章id)，成员按分值由大到小排列
        Set<String> ids = conn.zrevrange(order, start, end);
        List<Map<String,String>> articles = new ArrayList<Map<String,String>>();
        for (String id : ids){
            //hgetAll:通过key获取散列的所有键值对
            Map<String,String> articleData = conn.hgetAll(id);
            articleData.put("id", id);
            articles.add(articleData);
        }

        return articles;
    }

    /**
     * 文章分组
     * @param conn
     * @param articleId
     * @param toAdd
     */
    public void addGroups(Jedis conn, String articleId, String[] toAdd) {
        String article = "article:" + articleId;
        for (String group : toAdd) {
            conn.sadd("group:" + group, article);
        }
    }

    /**
     * 获取分组文章
     * @param conn
     * @param group
     * @param page
     * @return
     */
    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page) {
        return getGroupArticles(conn, group, page, "score:");
    }

    public List<Map<String,String>> getGroupArticles(Jedis conn, String group, int page, String order) {
        String key = order + group;
        if (!conn.exists(key)) {
            ZParams params = new ZParams().aggregate(ZParams.Aggregate.MAX);
            conn.zinterstore(key, params, "group:" + group, order);
            conn.expire(key, 60);
        }
        return getArticles(conn, page, key);
    }

    private void printArticles(List<Map<String,String>> articles){
        for (Map<String,String> article : articles){
            System.out.println("  id: " + article.get("id"));
            for (Map.Entry<String,String> entry : article.entrySet()){
                if (entry.getKey().equals("id")){
                    continue;
                }
                System.out.println("    " + entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
