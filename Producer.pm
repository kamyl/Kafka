package Kafka::Producer;

use strict;
use Kafka::Encode;
use Kafka::Connection;
use Carp;

=item new(host => ..., port => ...)

Create a new producer connecting to host and port

=cut

sub new {
    my $class = shift;
    my %args = @_;
    my $self = \%args;

    croak "host and port are required" if ( ! $self->{'host'} || ! $self->{'port'} );

    $self->{'request_key'} ||= 0;
    $self->{'compression'} ||= 0;
}

sub send {
    my $self = shift;
    my %args = @_;

    if ( ! $self->{'conn'} ) {
        $self->{'conn'} = Kafka::Connection->new(host => $self->{'host'}, port => $self->{'port'});
    }
    $self->{'conn'}->connect();
    return $self->{'conn'}->send(
        Kafka::Encode(
            request_id  => $self->{'request_key'},
            topic       => $args{'topic'},
            partition   => $args{'partition'} || 0xFFFFFFFF,
            mesages     => $args{'messages'},
            compression => $self->{'compression'}
        )
    );
}

1;
