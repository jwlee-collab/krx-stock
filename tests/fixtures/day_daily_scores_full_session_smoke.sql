INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume)
VALUES('005930','2026-05-07',100,106,99,104,10000000);
INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume)
VALUES('000660','2026-05-07',80,85,79,83,9000000);
INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume)
VALUES('035420','2026-05-07',200,212,198,208,7000000);

INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
VALUES('005930','2026-05-07',0.93,1);
INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
VALUES('000660','2026-05-07',0.89,2);
INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
VALUES('035420','2026-05-07',0.84,3);

INSERT OR IGNORE INTO daily_prices(symbol,date,open,high,low,close,volume)
VALUES('123456','2026-05-08',50,55,49,54,5000000);
INSERT OR REPLACE INTO daily_scores(symbol,date,score,rank)
VALUES('123456','2026-05-08',0.99,1);
