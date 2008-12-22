# $Id$

package Queue::Q4M::Fast;
use strict;
use warnings;

BEGIN
{
    my @attrs = qw(CONNECT_INFO DBH OPTIONS);
    my %constants;
    my $i = 0;
    foreach my $attr (@attrs) {
        $constants{ "Q4M_$attr" } = $i++;
    }

    require constant;
    constant->import(\%constats);
}

sub new {
    bless [
        undef, # connect_info
        undef, # dbh
        undef, # options hash
    ]
}

sub next {
    my ($self, @args) = @_;

    my $timeout;

    my $n_args = scalar @args;
    if ($args[-1] =~ /^\d+$/) {
        $timeout = pop @args;
    }

    my $dbh = $self->[ Q4M_DBH ];

    my $sql = sprintf(
        'SELECT queue_wait(%s)',
        join(', ', '?' x $n_args)
    );

    $sth->execute(@binds);
}

1;