# $Id: /mirror/perl/Queue-Q4M/trunk/lib/Queue/Q4M.pm 63615 2008-06-23T03:04:21.231823Z daisuke  $
#
# Copyright (c) 2008 Daisuke Maki <daisuke@endeworks.jp>
# All rights reserved.

package Queue::Q4M;
use Moose;

has 'connect_info' => (
    is => 'rw',
    isa => 'ArrayRef',
    required => 1,
);

has 'database' => (
    is => 'rw',
    isa => 'Str'
);

has 'sql_maker' => (
    is => 'rw',
    isa => 'SQL::Abstract',
    required => 1,
    default  => sub { SQL::Abstract->new }
);

has '_dbh' => (
    is => 'rw',
    isa => 'Maybe[DBI::db]',
);

has '_next_sth' => (
    is => 'rw',
    isa => 'Maybe[DBI::st]'
);

has '_next_args' => (
    is => 'rw',
    isa => 'ArrayRef',
    auto_deref => 1,
);

__PACKAGE__->meta->make_immutable;

no Moose;

use DBI;
use SQL::Abstract;

our $VERSION = '0.00002';


sub BUILD
{
    my $self = shift;

    my $connect_info = $self->connect_info;

    # XXX This is a hack. Hopefully it will be fixed in q4m
    if (! $self->database ) {
        $connect_info->[0] =~ /(?:dbname|database)=([^;]+)/;
        my $database = $1;
        $self->database($1);
    }
    $self;
}

sub connect
{
    my $self = shift;
    if (! ref $self) {
        $self = $self->new(@_);
    }

    $self->_dbh( $self->_connect() );
    $self;
}

sub _connect
{
    my $self = shift;

    return DBI->connect(@{ $self->connect_info });
}

sub dbh
{
    my $self = shift;
    my $dbh = $self->_dbh;

    if (! $dbh || ! $dbh->ping) {
        $dbh = $self->_connect();
        $self->_dbh( $dbh );
    }
    return $dbh;
}

sub next
{
    my $self = shift;
    my @args = @_;
    my @tables = 
        grep { !/^\d+$/ }
        map  {
            s/\[.*$//;
            $_
        }
        @args
    ;

    # Cache this statement handler so we don't unnecessarily create
    # string or handles
    my $sth = $self->_next_sth;
    if (! $sth) {
        my $dbh = $self->_dbh;
        my $sql = sprintf(
            "SELECT queue_wait(%s)",
            join(',', (('?') x scalar(@args)))
        );
        my $timeout = $args[-1] =~ /^\d+$/ ? pop @args  : undef;
        my @binds   = map {
            # if no dot exists, then add database\. to the beginning
            if ( index('.', $_) < 0 ) {
                $_ = join('.', $self->database, $_);
            }
            $_
        } @args;
        if ($timeout) {
            push @binds, $timeout;
        }

        $sth = $dbh->prepare( $sql ) ;
        $self->_next_sth( $sth );
        $self->_next_args( \@binds );
    }

    my $rv = $sth->execute($self->_next_args);
    my ($index) = $sth->fetchrow_array;
    $sth->finish;

    return $rv ?  $tables[$index - 1] : ()
}

sub _fetch_execute
{
    my $self = shift;
    my $table = shift;

    my ($sql, @bind) = $self->sql_maker->select($table, @_);
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare($sql);
    $sth->execute(@bind); # XXX - currently always empty
    return $sth;
}

*fetch = \&fetch_array;
sub fetch_array
{
    my $self = shift;
    my $sth  = $self->_fetch_execute(@_);
    my @ret  = $sth->fetchrow_array();
    $sth->finish;
    @ret;
}

sub fetch_arrayref
{
    my $self = shift;
    my $sth  = $self->_fetch_execute(@_);
    my $ret  = $sth->fetchrow_arrayref();
    $sth->finish;
    $ret;
}

sub fetch_hashref
{
    my $self = shift;
    my $sth  = $self->_fetch_execute(@_);
    my $ret  =  $sth->fetchrow_hashref();
    $sth->finish;
    $ret;
}

sub insert
{
    my $self  = shift;
    my $table = shift;

    my ($sql, @bind) = $self->sql_maker->insert($table, @_);
    my $dbh = $self->_dbh;
    my $sth = $dbh->prepare($sql);
    my $rv = $sth->execute(@bind);
    $sth->finish;
    return $rv;
}

sub disconnect
{
    my $self = shift;
    my $dbh  = $self->_dbh;
    if ($dbh) {
        $dbh->do("select queue_end()");
        $dbh->disconnect;
        $self->_dbh(undef);
    }
}

sub DEMOLISH
{
    my $self = shift;
    $self->disconnect;
}

1;

__END__

=head1 NAME

Queue::Q4M - Simple Interface To q4m

=head1 SYNOPSIS

  use Queue::Q4M;

  my $q = Queue::Q4M->connect(
    connect_info => [
      'dbi:mysql:dbname=mydb',
      $username,
      $password
    ],
  );

  for (1..10) {
    $q->insert($table, \%fieldvals);
  }

  while ($q->next($table)) {
    my ($col1, $col2, $col3) = $q->fetch($table, \@fields);
    print "col1 = $col1, col2 = $col2, col3 = $col3\n";
  }

  while ($q->next($table)) {
    my $cols = $q->fetch_arrayref($table, \@fields);
    print "col1 = $cols->[0], col2 = $cols->[1], col3 = $cols->[2]\n";
  }

  while ($q->next($table)) {
    my $cols = $q->fetch_hashref($table, \@fields);
    print "col1 = $cols->{col1}, col2 = $cols->{col2}, col3 = $cols->{col3}\n";
  }

  # to use queue_wait(table_cond1,table_cond2,timeout)
  while (my $which = $q->next_multi(@table_conds)) {
    # $which contains the table name
  }

  $q->disconnect;

=head1 DESCRIPTION

Queue::Q4M is a simple wrapper to q4m, which is an implementation of a queue
using mysql.

=head1 METHODS

=head2 new

Creates a new Queue::Q4M instance. Normally you should use connect() instead.

=head2 connect

Connects to the target database.

  my $q = Queue::Q4M->connect(
    connect_info => [
      'dbi:mysql:dbname=q4m',
    ]
  );

=head2 next($table_cond1[, $table_cond2, $table_cond3, ..., $timeout])

Blocks until the next item is available. This is equivalent to calling
queue_wait() on the given table.

  my $which = $q->next( $table_cond1, $table_cond2, $table_cond3 );

=head2 fetch

=head2 fetch_array

Fetches the next available row. Takes the list of columns to be fetched.

  my ($col1, $col2, $col3) = $q->fetch( $table, [ qw(col1 col2 col3) ] );

=head2 fetch_arrayref

Same as fetch_array, but fetches using fetchrow_arrayref()

  my $arrayref = $q->fetch_arrayref( $table, [ qw(col1 col2 col3) ] );

=head2 fetch_hashref

Same as fetch_array, but fetches using fetchrow_hashref()

  my $hashref = $q->fetch_hashref( $table, [ qw(col1 col2 col3) ] );

=head2 insert($table, \%field)

Inserts into the queue. The first argument should be a scalar specifying
a table name. The second argument is a hashref that specifies the mapping
between column names and their respective values.

  $q->insert($table, { col1 => $val1, col2 => $val2, col3 => $val3 });

For backwards compatibility, you may omit $table if you specified $table
in the constructor.

=head2 dbh

Returns the database handle after making sure that it's connected.

=head2 disconnect

Disconnects.

=head2 BUILD

=head2 DEMOLISH

These are defined as part of Moose infrastructure

=head1 AUTHOR

Copyright (c) 2008 Daisuke Maki E<lt>daisuke@endeworks.jpE<gt>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut