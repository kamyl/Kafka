package Kafka::Consumer;

use strict;
use Kafka::Connection;
use Kafka::Encode;
use Kafka::BoundedByteBuffer::Send;
use Kafka::BoundedByteBuffer::Recv;
use Kafka::MessageSet;
use Carp;
use Fcntl;

sub new {
    my $class = shift;
    my %args = @_;

    croak "no host / port" if ( ! $args{'host'} || ! $args{'port'} );

    my $self = {
        host    => $args{'host'},
        port    => $args{'port'},
        timeout => $args{'timeout'}
    };
    return bless $self;
}

sub fetch {
    my $self = shift;
    my $freq = shift;

    if ( ! $self->{'conn'} ) {
        $self->{'conn'} = Kafka::Connection->new( host => $self->{'host'}, port => $self->{'port'}, timeout => $self->{'timeout'} );
    }
    $self->send_request($freq);
    my $resp = $self->get_response();
    $self->close();
    return Kafka::MessageSet->new($resp->{'buffer'});
}

sub send_request {
    my $self = shift;
    my $freq = shift;

    my $send = Kafka::BoundedByteBuffer::Send->new($freq);
    $send->write_completely($self->{'conn'}{'conn'});
}

sub get_response {
    my $self = shift;

    my $resp = Kafka::BoundedByteBuffer::Recv->new();
    $resp->read_completely($self->{'conn'}{'conn'});
    return $resp;
}

sub close {
    my $self = shift;
    $self->{'conn'}->disconnect();
}

1;
