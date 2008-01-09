use strict;
use Test::More;

BEGIN
{
    if (! exists $ENV{Q4M_DSN} ||
        ! exists $ENV{Q4M_USER} ||
        ! exists $ENV{Q4M_PASSWORD} ||
        ! exists $ENV{Q4M_TABLE}
    ) {
        plan(skip_all => "Define environment variables Q4M_DSN, Q4M_USER, Q4M_PASSWORD and Q4M_TABLE");
    } else {
        plan(tests => 36);
    }
    use_ok("Queue::Q4M");
}


my $dsn      = $ENV{Q4M_DSN};
my $username = $ENV{Q4M_USER};
my $password = $ENV{Q4M_PASSWORD};
my $table    = $ENV{Q4M_TABLE};

my $q = Queue::Q4M->connect(
    table => $table,
    connect_info => [ $dsn, $username, $password ]
);
ok($q);
isa_ok($q, "Queue::Q4M");

my $max = 32;
for my $i (1..$max) {
    ok($q->insert({ v => $i }));
}

my $count = 0;
while ($q->next) {
    my $h = $q->fetch_hashref;
    $count++;
    last if $h->{v} == $max;
}

is($count, $max);
$q->disconnect;