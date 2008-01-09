# $Id: /mirror/perl/Queue-Q4M/trunk/lib/Queue/Q4M.pm 38280 2008-01-09T14:22:00.682385Z daisuke  $
#
# Copyright (c) 2008 Daisuke Maki <daisuke@endeworks.jp>
# All rights reserved.

package Queue::Q4M;
use strict;
use warnings;
use base qw(Class::Accessor::Fast);
use DBI;
use UNIVERSAL::require;

our $VERSION = '0.00001';

__PACKAGE__->mk_accessors($_) for qw(connect_info database table sql_maker _dbh _next_sth);

sub new
{
    my $class = shift;
    my %args  = @_;

    my $connect_info = $args{connect_info} || die "no connect_info specified";

    my $table = $args{table} || die "No table specified";

    my $sql_maker_class = $args{sql_maker_class} || 'SQL::Abstract';
    $sql_maker_class->require or die;
    my $sql_maker = $sql_maker_class->new();

    # XXX This is a hack. Hopefully it will be fixed in q4m
    $connect_info->[0] =~ /(?:dbname|database)=([^;]+)/;
    my $database = $1;

    $class->SUPER::new( {
        connect_info => $connect_info,
        table        => $table,
        database     => $database,
        sql_maker    => $sql_maker,
    });
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

    # Cache this statement handler so we don't unnecessarily create
    # string or handles
    my $sth = $self->_next_sth;
    if (! $sth) {
        my $dbh = $self->_dbh;
        $sth = $dbh->prepare(
            sprintf(
                "SELECT queue_wait(%s)",
                $dbh->quote(
                    join('.', $self->database, $self->table)
                )
            )
        );
        $self->_next_sth($sth);
    }
    my $rv = $sth->execute();
    $sth->finish;
    return $rv;
}

sub _fetch_execute
{
    my $self = shift;

    my ($sql, @bind) = $self->sql_maker->select($self->table, @_);
    my $dbh = $self->dbh;
    my $sth = $dbh->prepare_cached($sql);
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

    my ($sql, @bind) = $self->sql_maker->insert($self->table, @_);
    my $dbh = $self->_dbh;
    my $sth = $dbh->prepare_cached($sql);
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

sub DESTROY
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
    table => 'q4m'
  );

  for (1..10) {
    $q->insert(\%fieldvals);
  }

  while ($q->next) {
    my ($col1, $col2, $col3) = $q->fetch(\@fields);
    print "col1 = $col1, col2 = $col2, col3 = $col3\n";
  }

  while ($q->next) {
    my $cols = $q->fetch_arrayref(\@fields);
    print "col1 = $cols->[0], col2 = $cols->[1], col3 = $cols->[2]\n";
  }

  while ($q->next) {
    my $cols = $q->fetch_hashref(\@fields);
    print "col1 = $cols->{col1}, col2 = $cols->{col2}, col3 = $cols->{col3}\n";
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
    table => 'q4m',
    connect_info => [
      'dbi:mysql:dbname=q4m',
    ]
  );

=head2 next

Blocks until the next item is available. This is equivalent to calling
queue_wait() on the given table.

=head2 fetch

=head2 fetch_array

Fetches the next available row. Takes the list of columns to be fetched.

  my ($col1, $col2, $col3) = $q->fetch( [ qw(col1 col2 col3) ] );

=head2 fetch_arrayref

Same as fetch_array, but fetches using fetchrow_arrayref()

  my $arrayref = $q->fetch_arrayref( [ qw(col1 col2 col3) ] );

=head2 fetch_hashref

Same as fetch_array, but fetches using fetchrow_hashref()

  my $hashref = $q->fetch_arrayref( [ qw(col1 col2 col3) ] );

=head2 insert

Inserts into the queue.

  $q->insert({ col1 => $val1, col2 => $val2, col3 => $val3 });

=head2 dbh

Returns the database handle after making sure that it's connected.

=head2 disconnect

Disconnects.

=head1 AUTHOR

Copyright (c) 2008 Daisuke Maki E<lt>daisuke@endeworks.jpE<gt>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See http://www.perl.com/perl/misc/Artistic.html

=cut