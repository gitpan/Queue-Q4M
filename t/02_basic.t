use strict;
use Test::More;

BEGIN
{
    if (! exists $ENV{Q4M_DSN} ) {
        plan(skip_all => "Define environment variables Q4M_DSN, and optionally Q4M_USER and Q4M_PASSWORD as appropriate");
    } else {
        plan(tests => 45);
    }
    use_ok("Queue::Q4M");
}


my $dsn      = $ENV{Q4M_DSN};
my $username = $ENV{Q4M_USER};
my $password = $ENV{Q4M_PASSWORD};
my @tables   = map {
    join('_', qw(q4m test table), $_, $$)
} 1..10;

if ($dsn !~ /^dbi:mysql:/i) {
    $dsn = "dbi:mysql:dbname=$dsn";
}

my $dbh = DBI->connect($dsn, $username, $password);
foreach my $table (@tables) {
    $dbh->do(<<EOSQL);
        CREATE TABLE IF NOT EXISTS $table (
            v INTEGER NOT NULL
        ) ENGINE=queue;
EOSQL
}

{
    my $table = $tables[0];
    my $q = Queue::Q4M->connect(
        connect_info => [ $dsn, $username, $password ]
    );
    ok($q);
    isa_ok($q, "Queue::Q4M");
    
    my $max = 32;
    for my $i (1..$max) {
        ok($q->insert($table, { v => $i }));
    }
    
    my $count = 0;
    while ($q->next($table)) {
        my $h = $q->fetch_hashref();
        $count++;
        last if $h->{v} == $max;
    }
    
    is($count, $max);
    $q->disconnect;
}

{
    my $table = $tables[0];
    my $q = Queue::Q4M->connect(
        table => $table,
        connect_info => [ $dsn, $username, $password ]
    );
    ok($q);
    isa_ok($q, "Queue::Q4M");

    diag("Going to block for 5 seconds...");
    my $before = time();
    $q->next($table, 5);

    # This time difference could be off by a second or so,
    # so allow that much diffference
    my $diff = time() - $before;
    ok( $diff >= 4, "next() with timeout waited for 4 seconds ($diff)");
}

{
    my $q = Queue::Q4M->connect(
        connect_info => [ $dsn, $username, $password ]
    );
    ok($q);
    isa_ok($q, "Queue::Q4M");

    # Insert into a random table
    my $table = $tables[rand(@tables)];
    $q->insert( $table , { v => 1 } );

    my $max = 1;
    my $count = 0;
    while (my $which = $q->next(@tables, 5)) {
        is ($which, $table, "got from the table that we inserted" );
        my ($v) = $q->fetch( $which, 'v' );
        $count++;
        last if $count >= $max;
    }
}

{
    my $table   = $tables[0];
    my $timeout = 1;
    my $q = Queue::Q4M->connect(
        connect_info => [ $dsn, $username, $password ]
    );
    ok($q);
    isa_ok($q, "Queue::Q4M");

    my $rv = $q->next($table, $timeout);
    ok( ! $rv, "should return false. got (" . ($rv || '') . ")" );

    $q->disconnect;
}


END
{
    local $@;
    eval {
        foreach my $table (@tables) {
            $dbh->do("DROP TABLE $table");
        }   
    };
}