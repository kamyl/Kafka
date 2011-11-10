package Kafka::FetchRequest;

use strict;
use Kafka::RequestKeys;
use Carp;

sub new {
    my $class = shift;
    my %args = @_;
    my $self = {
        id          => $Kafka::RequestKeys::FETCH,
        topic       => $args{'topic'},
        partition   => $args{'partition'} || 0,
        offset      => $args{'offset'} || 0,
        maxsize     => $args{'maxsize'} || 1000000
    };
    return bless $self;
}

sub write_to {
    my $self = shift;
    my %args = @_;

    my ($i1, $i2) = (int($self->{'offset'}/2**32)%2**32,int($self->{'offset'}%2**32));

    syswrite($args{'stream'}, pack('n', length($self->{'topic'})) . $self->{'topic'});
    syswrite($args{'stream'}, pack('N', $self->{'partition'}));
    syswrite($args{'stream'}, pack('NN', $i1, $i2));
    syswrite($args{'stream'}, pack('N', $self->{'maxsize'}));
}

sub size_in_bytes {
    my $self = shift;
    return 2 + length($self->{'topic'}) + 4 + 8 + 4;
}

sub to_string {
    my $self = shift;
    return 'id:' . $self->{'id'} . ' topic:' . $self->{'topic'} . ' part:' . $self->{'partition'} . ' offset:' . $self->{'offset'} . ' maxsize:' . $self->{'maxsize'};
}

1;
