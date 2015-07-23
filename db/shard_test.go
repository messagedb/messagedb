package db

// // Ensure the shard will automatically flush the WAL after a threshold has been reached.
// func TestShard_Autoflush(t *testing.T) {
// 	path, _ := ioutil.TempDir("", "shard_test")
// 	defer os.RemoveAll(path)
//
// 	// Open shard with a really low size threshold, high flush interval.
// 	sh := NewShard(NewDatabaseIndex(), filepath.Join(path, "shard"))
// 	sh.MaxWALSize = 1024 // 1KB
// 	sh.WALFlushInterval = 1 * time.Hour
// 	sh.WALPartitionFlushDelay = 1 * time.Millisecond
// 	if err := sh.Open(); err != nil {
// 		t.Fatal(err)
// 	}
// 	defer sh.Close()
//
// 	// Write a bunch of points.
// 	for i := 0; i < 100; i++ {
// 		if err := sh.WriteMessages([]Message{NewMessage(time.Unix(1, 2))}); err != nil {
// 			t.Fatal(err)
// 		}
// 	}
//
// 	// Wait for autoflush.
// 	time.Sleep(100 * time.Millisecond)
//
// 	// Make sure we have series buckets created outside the WAL.
// 	if n, err := sh.ConversationsCount(); err != nil {
// 		t.Fatal(err)
// 	} else if n < 10 {
// 		t.Fatalf("not enough series, expected at least 10, got %d", n)
// 	}
// }
