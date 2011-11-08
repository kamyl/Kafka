package Kafka::Connection;

use strict;
use IO::Socket::INET;
use Carp;

=item new(host => ..., port => ...)

Connects to Kafka on the given host / port combo

=cut

sub new {
    my $class = shift;
    my %args = @_;
    croak "host and post required" if ( ! $args{'host'} || ! $args{'port'} );

    my $self = \%args;
    bless $self;

    $self->connect();
    return $self;
}

=item connect()

connects to kafka

=cut

sub connect {
    my $self = shift;
    if ( ! $self->{'conn'} || ! $self->{'conn'}->connected() ) {
        $self->{'conn'} = IO::Socket::INET->new(
            PeerAddr    => $self->{'host'},
            PeerPort    => $self->{'port'},
            Proto       => 'tcp',
            Blocking    => (defined $self->{'blocking'}) ? $self->{'blocking'} : 1,
            Timeout     => $self->{'timeout'}
        );
    }

    if ( ! $self->{'conn'} || ! $self->{'conn'}->connected() ) {
        croak "Could not connect";
    }
}

sub send {
    my $self = shift;
    my $data = shift;
    return $self->{'conn'}->send($data);
}

sub recv {
    my $self = shift;
    my $data;
    while ( <$self->{'conn'}> ) {
        $data .= $_;
    }
    return $data;
}

sub disconnect {
    my $self = shift;
    if ( $self->{'conn'} ) {
        close($self->{'conn'});
    }
}

1;
