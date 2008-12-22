use strict;
use Test::More;

BEGIN {
    eval { require Parallel::ForkManager };
    if ($@) {
        plan( skip_all => "Test requires Parallell::ForkManager" );
    } else {
        plan( tests => 3);
    }
}

my $pm = Parallel::ForkManager->new(5);