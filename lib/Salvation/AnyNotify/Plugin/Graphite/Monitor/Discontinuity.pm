package Salvation::AnyNotify::Plugin::Graphite::Monitor::Discontinuity;

use strict;
use warnings;
use bignum;

use base 'Salvation::AnyNotify::Plugin::Graphite::Monitor';

use JSON ();
use Salvation::Method::Signatures;

use constant {

    POINT_IDX_TIME => 1,
    POINT_IDX_VALUE => 0,
};

sub default_memory_limit { 100 }

method add(
    Str{1,}|ArrayRef[Str{1,}]{1,} :target!, Int :threshold,
    Str{1,} :from, Str{1,} :to, Int :memory
) {

    my $core = $self -> core();
    my %data = ();

    $from //= '-10min';
    $to //= 'now';
    $memory //= $self -> default_memory_limit();

    return sub {

        my $now = time();
        my $result = $core -> graphite() -> query(
            target => $target,
            from => $from,
            to => $to,
        );

        if( defined $result ) {

            my @warnings = ();

            foreach my $metric ( $result -> all_metrics() ) {

                my $target = $metric -> target();
                my $storage = $data{ $target } //= {
                    sum => 0,
                    cnt => 0,
                    avg => 0,
                    sum_of_squared_ranges => 0,
                    sum_of_cubed_ranges => 0,
                    m3 => 0,
                    stddev => 0,
                    skew => 0,
                    ( defined $threshold ? ( threshold => $threshold ) : () ),
                    values => [],
                    prev_value => undef,
                    diff => 0,
                };

                my $warn = sub {

                    my ( $type, %data ) = @_;

                    push( @warnings, {
                        time => $now,
                        type => "graphite/discontinuity/${type}",
                        data => \%data,
                        stat => { map( { $_ => $storage -> { $_ } } (
                            'avg', 'stddev', 'skew',
                        ) ) },
                        target => $target,
                    } );
                };

                foreach my $point ( sort( {
                        ( $a -> [ POINT_IDX_TIME ] // 0 )
                        <=> ( $b -> [ POINT_IDX_TIME ] // 0 ) } @{ $metric -> datapoints() } ) ) {

                    my $value = $point -> [ POINT_IDX_VALUE ];

                    next unless defined $value;

                    ++ $storage -> { 'cnt' };
                    $storage -> { 'sum' } += $value;

                    $storage -> { 'avg' } = ( $storage -> { 'sum' } / $storage -> { 'cnt' } );

                    if( defined $storage -> { 'prev_value' } ) {

                        $storage -> { 'diff' } = abs( $value - $storage -> { 'prev_value' } );

                        if(
                            exists $storage -> { 'threshold' }
                            && ( $storage -> { 'diff' } >= $storage -> { 'threshold' } )
                        ) {

                            $warn -> ( threshold => (
                                diff => $storage -> { 'diff' },
                                threshold => $storage -> { 'threshold' },
                            ) );
                        }
                    }

                    $storage -> { 'prev_value' } = $value;
                    push( @{ $storage -> { 'values' } }, $value );

                    while( scalar( @{ $storage -> { 'values' } } ) > $memory ) {

                        shift( @{ $storage -> { 'values' } } )
                    }
                }

                foreach my $value ( @{ $storage -> { 'values' } } ) {

                    $storage -> { 'sum_of_squared_ranges' } += (
                        ( $value - $storage -> { 'avg' } ) ** 2
                    );

                    $storage -> { 'sum_of_cubed_ranges' } += (
                        ( $value - $storage -> { 'avg' } ) ** 3
                    );
                }

                $storage -> { 'm3' } = (
                    $storage -> { 'sum_of_cubed_ranges' } / $storage -> { 'cnt' }
                );

                $storage -> { 'stddev' } = sqrt(
                    $storage -> { 'sum_of_squared_ranges' } / $storage -> { 'cnt' }
                );

                $storage -> { 'skew' } = (
                    $storage -> { 'm3' } / ( $storage -> { 'stddev' } ** 3 )
                );

                if( $storage -> { 'stddev' } > 0 ) {

                    my $highest_z = undef;
                    my $most_abnormal_value = undef;

                    foreach my $value ( @{ $storage -> { 'values' } } ) {

                        my $z = ( ( $value - $storage -> { 'avg' } ) / $storage -> { 'stddev' } );

                        if( defined $highest_z ) {

                            if( $z > $highest_z ) {

                                $highest_z = $z;
                                $most_abnormal_value = $value;
                            }

                        } else {

                            $highest_z = $z;
                            $most_abnormal_value = $value;
                        }
                    }

                    if( defined $highest_z && ( $highest_z > 2 ) ) {

                        $warn -> ( abnormal => (
                            z => $highest_z,
                            most_abnormal_value => $most_abnormal_value,
                        ) );
                    }
                }
            }

            if( scalar( @warnings ) > 0 ) {

                my $bus = $core -> bus();
                my $json = JSON
                    -> new()
                    -> allow_blessed()
                    -> allow_bignum()
                ;

                foreach my $warning ( @warnings ) {

                    $bus -> notify( $warning -> { 'type' }, $json -> encode( $warning ) );
                }
            }
        }
    };
}

1;

__END__
