# $Id$

package Queue::Q4M::Status;
use Moose;
use DBI;
use Carp();
no Moose;

sub fetch {
    # Black magic. Since I don't know if the parameters will be the same
    # for every future release of Q4M, I'll just create a cached
    # anonymous class via moose. Wicked!

    my $class = shift;
    my $dbh   = shift || Carp::confess("Q4M requires a database handle");

    my $sth = $dbh->prepare( "SHOW ENGINE QUEUE STATUS" );

    $sth->execute();
    my %attributes;

    my ($dummy1, $dummy2, $status);
    $sth->bind_columns(\($dummy1, $dummy2, $status));
    $sth->fetchrow_arrayref;
    $sth->finish;

    # now parse the status
    foreach my $line (split(/\r?\n/, $status)) {
        next unless $line =~ /^([\w_]+)\s+(\d+)$/;
        $attributes{$1} = $2;
    }

    my $metaclass = Moose::Meta::Class->create_anon_class(
        superclasses => [ 'Queue::Q4M::Status' ],
        cache => 1,
        attributes => [
            map { Moose::Meta::Attribute->new($_ => ( is => 'rw') ) }
                keys %attributes
        ] 
    );   

    $metaclass->new_object( %attributes );
}
             
1;

__END__

=head1 NAME

Queue::Q4M::Status - A Q4M Status Object

=head1 SYNPSIS

  my $q4m = Queue::Q4M->connect(...);
  my $status = $q4m->status;

  print "rows written: ", $status->rows_written, "\n";

=head1 DESCRIPTION

For a list of possible attributes (such as rows_written) please see the
actual q4m manual. This module will parse the output of 

  SHOW ENGINE QUEUE STATUS

and generates accessors for these attributes automatically

=head1 METHODS

=head2 fetch($dbh)

Given a database handle, creates a Queue::Q4M::Status instance

=cut
